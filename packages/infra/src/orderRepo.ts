import type { OrderStatus } from '@fss/core';
import type { Pool, PoolClient } from 'pg';

/** Consumer name recorded in `processed_messages` for stream deliveries. */
export const RESERVATION_CONSUMER = 'orchestrator.reservations';

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
 * Persists a reservation as a RESERVED order and appends the saga event, in one
 * transaction. The order id is the reservation id from Redis, so redelivery is
 * idempotent: `ON CONFLICT (id) DO NOTHING` never creates a second order.
 */
export async function persistReservation(pool: Pool, reservation: Reservation): Promise<boolean> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const inserted = await client.query<{ id: string }>(
      `INSERT INTO orders (id, sale_id, user_id, status)
       VALUES ($1, $2, $3, 'RESERVED')
       ON CONFLICT (id) DO NOTHING
       RETURNING id`,
      [reservation.orderId, reservation.saleId, reservation.userId],
    );

    if (inserted.rowCount === 1) {
      await appendSagaEvent(client, reservation.orderId, 'order.reserved', {
        saleId: reservation.saleId,
        userId: reservation.userId,
        reservedAt: reservation.reservedAt,
      });
    }

    await client.query(
      `INSERT INTO processed_messages (consumer, message_id)
       VALUES ($1, $2)
       ON CONFLICT DO NOTHING`,
      [RESERVATION_CONSUMER, reservation.orderId],
    );

    await client.query('COMMIT');
    return inserted.rowCount === 1;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Phase 2 payment stub: payment succeeds immediately, so a RESERVED order is
 * marked COMPLETED. The conditional UPDATE makes a redelivered reservation a
 * no-op, and the event is only appended when the transition actually happened.
 */
export async function completeOrderStub(pool: Pool, orderId: string): Promise<boolean> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const updated = await client.query<{ id: string }>(
      `UPDATE orders
          SET status = 'COMPLETED',
              updated_at = now()
        WHERE id = $1 AND status = 'RESERVED'
        RETURNING id`,
      [orderId],
    );

    if (updated.rowCount === 1) {
      await appendSagaEvent(client, orderId, 'order.completed', { stub: true });
    }

    await client.query('COMMIT');
    return updated.rowCount === 1;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
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
