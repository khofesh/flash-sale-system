import { nextAction, type OrderStatus } from '@fss/core';
import type { Pool, PoolClient } from 'pg';

/** Consumer names recorded in `processed_messages`. */
export const RESERVATION_CONSUMER = 'orchestrator.reservations';
export const ORDER_EVENTS_CONSUMER = 'orchestrator.order-events';

export interface OrderRecord {
  id: string;
  saleId: string;
  userId: string;
  status: OrderStatus;
  paymentRef: string | null;
  shipmentRef: string | null;
  failureReason: string | null;
  createdAt: Date;
  updatedAt: Date;
}

interface OrderRow {
  id: string;
  sale_id: string;
  user_id: string;
  status: OrderStatus;
  payment_ref: string | null;
  shipment_ref: string | null;
  failure_reason: string | null;
  created_at: Date;
  updated_at: Date;
}

const orderColumns =
  'id, sale_id, user_id, status, payment_ref, shipment_ref, failure_reason, created_at, updated_at';

export interface Reservation {
  saleId: string;
  orderId: string;
  userId: string;
  /** Reservation timestamp from the stream entry, in epoch milliseconds. */
  reservedAt: number;
}

/**
 * How a reservation delivery was handled:
 * - `inserted`: a new RESERVED order was written.
 * - `duplicate`: this reservation was already processed (stream redelivery).
 * - `conflict`: the user already holds another live order for the sale, so the
 *   partial unique index refused a second one. The caller must give the unit
 *   back to stock; this outcome is only returned once per reservation.
 */
export type PersistOutcome = 'inserted' | 'duplicate' | 'conflict';

/**
 * Persists a reservation as a RESERVED order and appends the saga event, in one
 * transaction. Deliveries are deduplicated through `processed_messages`, and the
 * order id is the reservation id from Redis, so redelivery never creates a
 * second order. `ON CONFLICT DO NOTHING` also covers the one-live-order-per-user
 * index, so a conflicting entry is recorded instead of failing forever.
 */
export async function persistReservation(
  pool: Pool,
  reservation: Reservation,
): Promise<PersistOutcome> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const claimed = await client.query(
      `INSERT INTO processed_messages (consumer, message_id)
       VALUES ($1, $2)
       ON CONFLICT DO NOTHING`,
      [RESERVATION_CONSUMER, reservation.orderId],
    );
    if (claimed.rowCount !== 1) {
      await client.query('COMMIT');
      return 'duplicate';
    }

    const inserted = await client.query<{ id: string }>(
      `INSERT INTO orders (id, sale_id, user_id, status)
       VALUES ($1, $2, $3, 'RESERVED')
       ON CONFLICT DO NOTHING
       RETURNING id`,
      [reservation.orderId, reservation.saleId, reservation.userId],
    );

    if (inserted.rowCount === 1) {
      await appendSagaEvent(client, reservation.orderId, 'order.reserved', {
        saleId: reservation.saleId,
        userId: reservation.userId,
        reservedAt: reservation.reservedAt,
      });
      await client.query('COMMIT');
      return 'inserted';
    }

    // The order id already exists: an earlier delivery wrote it without a
    // processed_messages row. Treat it as a redelivery, never as a conflict.
    const existing = await client.query('SELECT 1 FROM orders WHERE id = $1', [
      reservation.orderId,
    ]);
    await client.query('COMMIT');
    return existing.rowCount === 1 ? 'duplicate' : 'conflict';
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export interface OrderTransition {
  orderId: string;
  from: OrderStatus;
  to: OrderStatus;
  /** Audit event type, e.g. `order.paid`. */
  type: string;
  payload?: unknown;
  patch?: {
    paymentRef?: string;
    shipmentRef?: string;
    failureReason?: string;
  };
}

/**
 * Applies one conditional state transition and appends its saga event atomically
 * (`UPDATE ... WHERE id = $1 AND status = $expected`, SPEC §4.2). Returns false
 * when the order was not in the expected state: a duplicate or out-of-order
 * message that must not be applied again.
 */
export async function transition(pool: Pool, input: OrderTransition): Promise<boolean> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const assignments = ['status = $2', 'updated_at = now()'];
    const values: unknown[] = [input.orderId, input.to];

    if (input.patch?.paymentRef !== undefined) {
      values.push(input.patch.paymentRef);
      assignments.push(`payment_ref = $${values.length}`);
    }
    if (input.patch?.shipmentRef !== undefined) {
      values.push(input.patch.shipmentRef);
      assignments.push(`shipment_ref = $${values.length}`);
    }
    if (input.patch?.failureReason !== undefined) {
      values.push(input.patch.failureReason);
      assignments.push(`failure_reason = $${values.length}`);
    }

    values.push(input.from);
    const expected = `$${values.length}`;

    const updated = await client.query<{ id: string }>(
      `UPDATE orders
          SET ${assignments.join(', ')}
        WHERE id = $1 AND status = ${expected}
        RETURNING id`,
      values,
    );

    const applied = updated.rowCount === 1;
    if (applied) {
      await appendSagaEvent(client, input.orderId, input.type, input.payload ?? {});
    }

    await client.query('COMMIT');
    return applied;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

/** Appends an event to the audit log without changing status. */
export async function recordSagaEvent(
  pool: Pool,
  orderId: string,
  type: string,
  payload: unknown = {},
): Promise<void> {
  await pool.query(
    `INSERT INTO saga_events (order_id, type, payload)
     VALUES ($1, $2, $3::jsonb)`,
    [orderId, type, JSON.stringify(payload)],
  );
}

export async function isMessageProcessed(
  pool: Pool,
  consumer: string,
  messageId: string,
): Promise<boolean> {
  const result = await pool.query(
    'SELECT 1 FROM processed_messages WHERE consumer = $1 AND message_id = $2',
    [consumer, messageId],
  );
  return result.rowCount === 1;
}

/**
 * Marks a message as consumed. Called after every side effect succeeded, so a
 * crash mid-handling leaves it unmarked and SQS redelivers it.
 */
export async function markMessageProcessed(
  pool: Pool,
  consumer: string,
  messageId: string,
): Promise<void> {
  await pool.query(
    `INSERT INTO processed_messages (consumer, message_id)
     VALUES ($1, $2)
     ON CONFLICT DO NOTHING`,
    [consumer, messageId],
  );
}

export interface OrderRef {
  id: string;
  saleId: string;
  userId: string;
}

/**
 * Cancels PAYMENT_PENDING orders older than `timeoutMs` and records the event.
 * `FOR UPDATE SKIP LOCKED` lets several orchestrator replicas sweep at once
 * without fighting over the same rows (SPEC §4.2). The state machine decides the
 * transition. Returning the unit is left to `findUnreleasedOrders`, so a crash
 * right after this commit cannot lose the release.
 */
export async function cancelTimedOutPayments(
  pool: Pool,
  timeoutMs: number,
  limit = 100,
): Promise<OrderRef[]> {
  const step = nextAction({ status: 'PAYMENT_PENDING' }, { type: 'payment.timeout' }).find(
    (candidate) => candidate.kind === 'transition',
  );
  if (step?.kind !== 'transition') {
    throw new Error('The state machine has no transition for payment.timeout');
  }
  const reason = step.patch?.failureReason ?? null;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const expired = await client.query<{ id: string; sale_id: string; user_id: string }>(
      `SELECT id, sale_id, user_id
         FROM orders
        WHERE status = 'PAYMENT_PENDING'
          AND updated_at < now() - ($1::int * interval '1 millisecond')
        ORDER BY updated_at
        LIMIT $2
        FOR UPDATE SKIP LOCKED`,
      [timeoutMs, limit],
    );

    if (expired.rowCount === 0) {
      await client.query('COMMIT');
      return [];
    }

    const ids = expired.rows.map((row) => row.id);
    const updated = await client.query<{ id: string; sale_id: string; user_id: string }>(
      `UPDATE orders
          SET status = $2,
              failure_reason = $3,
              updated_at = now()
        WHERE id = ANY($1::uuid[]) AND status = $4
        RETURNING id, sale_id, user_id`,
      [ids, step.to, reason, step.from],
    );

    if (updated.rows.length > 0) {
      const cancelled = updated.rows.map((row) => row.id);
      await client.query(
        `INSERT INTO saga_events (order_id, type, payload)
         SELECT id, $2, $3::jsonb
           FROM unnest($1::uuid[]) AS id`,
        [cancelled, step.type, JSON.stringify({ reason })],
      );
    }

    await client.query('COMMIT');
    return updated.rows.map((row) => ({
      id: row.id,
      saleId: row.sale_id,
      userId: row.user_id,
    }));
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

/** CANCELLED/REFUNDED orders whose unit has not been confirmed back in Redis. */
export async function findUnreleasedOrders(pool: Pool, limit = 100): Promise<OrderRef[]> {
  const result = await pool.query<{ id: string; sale_id: string; user_id: string }>(
    `SELECT id, sale_id, user_id
       FROM orders
      WHERE status IN ('CANCELLED', 'REFUNDED') AND released_at IS NULL
      ORDER BY updated_at
      LIMIT $1`,
    [limit],
  );
  return result.rows.map((row) => ({ id: row.id, saleId: row.sale_id, userId: row.user_id }));
}

/** Records that the order's unit is back in Redis. */
export async function markOrderReleased(pool: Pool, orderId: string): Promise<void> {
  await pool.query('UPDATE orders SET released_at = now() WHERE id = $1 AND released_at IS NULL', [
    orderId,
  ]);
}

/** Most recent order for a user, used by `GET /api/purchases/:userId`. */
export async function getOrderByUser(
  pool: Pool,
  saleId: string,
  userId: string,
): Promise<OrderRecord | undefined> {
  const result = await pool.query<OrderRow>(
    `SELECT ${orderColumns}
       FROM orders
      WHERE sale_id = $1 AND user_id = $2
      ORDER BY created_at DESC, id DESC
      LIMIT 1`,
    [saleId, userId],
  );

  const row = result.rows[0];
  return row ? toRecord(row) : undefined;
}

export async function getOrderById(pool: Pool, orderId: string): Promise<OrderRecord | undefined> {
  const result = await pool.query<OrderRow>(`SELECT ${orderColumns} FROM orders WHERE id = $1`, [
    orderId,
  ]);
  const row = result.rows[0];
  return row ? toRecord(row) : undefined;
}

async function appendSagaEvent(
  client: PoolClient,
  orderId: string,
  type: string,
  payload: unknown,
): Promise<void> {
  await client.query(
    `INSERT INTO saga_events (order_id, type, payload)
     VALUES ($1, $2, $3::jsonb)`,
    [orderId, type, JSON.stringify(payload)],
  );
}

function toRecord(row: OrderRow): OrderRecord {
  return {
    id: row.id,
    saleId: row.sale_id,
    userId: row.user_id,
    status: row.status,
    paymentRef: row.payment_ref,
    shipmentRef: row.shipment_ref,
    failureReason: row.failure_reason,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}
