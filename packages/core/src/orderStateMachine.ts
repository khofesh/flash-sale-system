import { randomUUID } from 'node:crypto';
import type { OrderStatus } from './purchase.js';

/** Every message that can travel between saga participants (SPEC §7). */
export const SAGA_MESSAGE_TYPES = [
  'payment.requested',
  'payment.refund_requested',
  'shipping.requested',
  'payment.succeeded',
  'payment.failed',
  'payment.refunded',
  'shipping.succeeded',
  'shipping.failed',
] as const;

export type SagaMessageType = (typeof SAGA_MESSAGE_TYPES)[number];

export type SagaQueueName = 'payment-requests' | 'shipping-requests' | 'order-events';

/** The queue an orchestrator-produced message belongs to (SPEC §7). */
export const QUEUE_FOR_MESSAGE: Readonly<Record<SagaMessageType, SagaQueueName>> = {
  'payment.requested': 'payment-requests',
  'payment.refund_requested': 'payment-requests',
  'shipping.requested': 'shipping-requests',
  'payment.succeeded': 'order-events',
  'payment.failed': 'order-events',
  'payment.refunded': 'order-events',
  'shipping.succeeded': 'order-events',
  'shipping.failed': 'order-events',
};

/** Events the orchestrator acts on. */
export type SagaEventType =
  | 'reservation.persisted'
  | 'payment.succeeded'
  | 'payment.failed'
  | 'payment.refunded'
  | 'shipping.succeeded'
  | 'shipping.failed'
  | 'payment.timeout';

/** Events that arrive from payment/shipping over `order-events`. */
export const SAGA_RESULT_EVENT_TYPES: readonly SagaEventType[] = [
  'payment.succeeded',
  'payment.failed',
  'payment.refunded',
  'shipping.succeeded',
  'shipping.failed',
];

export interface SagaEnvelope<T = Record<string, unknown>> {
  messageId: string;
  orderId: string;
  type: SagaMessageType;
  attempt: number;
  occurredAt: string;
  payload: T;
}

export interface SagaEvent {
  type: SagaEventType;
  payload?: Record<string, unknown>;
}

export interface OrderSnapshot {
  status: OrderStatus;
}

export interface OrderPatch {
  paymentRef?: string;
  shipmentRef?: string;
  failureReason?: string;
}

/** One step of the plan returned by `nextAction`. */
export type SagaStep =
  | { kind: 'transition'; from: OrderStatus; to: OrderStatus; type: string; patch?: OrderPatch }
  | { kind: 'publish'; type: SagaMessageType }
  | { kind: 'release'; reason: string };

/**
 * Legal single transitions (SPEC §4.2). `PAID -> SHIPPING_PENDING` is reached
 * with a second `payment.succeeded`, so a crash between the two conditional
 * updates is repaired by redelivering the same event.
 */
export const ORDER_TRANSITIONS: Readonly<
  Record<OrderStatus, Partial<Record<SagaEventType, OrderStatus>>>
> = {
  RESERVED: { 'reservation.persisted': 'PAYMENT_PENDING' },
  PAYMENT_PENDING: {
    'payment.succeeded': 'PAID',
    'payment.failed': 'CANCELLED',
    'payment.timeout': 'CANCELLED',
  },
  PAID: { 'payment.succeeded': 'SHIPPING_PENDING' },
  SHIPPING_PENDING: {
    'shipping.succeeded': 'COMPLETED',
    'shipping.failed': 'SHIPPING_FAILED',
  },
  SHIPPING_FAILED: { 'payment.refunded': 'REFUNDED' },
  COMPLETED: {},
  CANCELLED: {},
  REFUNDED: {},
};

export function transitionTarget(
  status: OrderStatus,
  event: SagaEventType,
): OrderStatus | undefined {
  return ORDER_TRANSITIONS[status][event];
}

/**
 * Pure saga reducer. Given the current order status and an event, returns the
 * steps to apply in order. The result is idempotent: re-running it against the
 * state it produced only emits publish/release effects that are safe to repeat.
 */
export function nextAction(order: OrderSnapshot, event: SagaEvent): SagaStep[] {
  const { status } = order;

  switch (event.type) {
    case 'reservation.persisted': {
      const to = transitionTarget(status, event.type);
      if (to) {
        return [
          { kind: 'transition', from: status, to, type: 'order.payment_pending' },
          { kind: 'publish', type: 'payment.requested' },
        ];
      }
      // Redelivery after a crash between the transition and the publish.
      if (status === 'PAYMENT_PENDING') return [{ kind: 'publish', type: 'payment.requested' }];
      return [];
    }

    case 'payment.succeeded': {
      const patch = referencePatch('paymentRef', event.payload);

      if (status === 'PAYMENT_PENDING') {
        return [
          { kind: 'transition', from: 'PAYMENT_PENDING', to: 'PAID', type: 'order.paid', patch },
          {
            kind: 'transition',
            from: 'PAID',
            to: 'SHIPPING_PENDING',
            type: 'order.shipping_pending',
          },
          { kind: 'publish', type: 'shipping.requested' },
        ];
      }
      if (status === 'PAID') {
        return [
          {
            kind: 'transition',
            from: 'PAID',
            to: 'SHIPPING_PENDING',
            type: 'order.shipping_pending',
          },
          { kind: 'publish', type: 'shipping.requested' },
        ];
      }
      if (status === 'SHIPPING_PENDING') {
        return [{ kind: 'publish', type: 'shipping.requested' }];
      }
      // The payment succeeded after the sweeper cancelled the order: refund it.
      if (status === 'CANCELLED') {
        return [{ kind: 'publish', type: 'payment.refund_requested' }];
      }
      return [];
    }

    case 'payment.failed':
    case 'payment.timeout': {
      const reason =
        event.type === 'payment.timeout'
          ? 'PAYMENT_TIMEOUT'
          : (stringField(event.payload, 'reason') ?? 'PAYMENT_DECLINED');

      if (status === 'PAYMENT_PENDING') {
        return [
          {
            kind: 'transition',
            from: 'PAYMENT_PENDING',
            to: 'CANCELLED',
            type: 'order.cancelled',
            patch: { failureReason: reason },
          },
          { kind: 'release', reason },
        ];
      }
      // Redelivery after a crash between the cancel commit and the release.
      if (status === 'CANCELLED') return [{ kind: 'release', reason }];
      return [];
    }

    case 'shipping.succeeded': {
      const patch = referencePatch('shipmentRef', event.payload);
      if (status === 'SHIPPING_PENDING') {
        return [
          {
            kind: 'transition',
            from: 'SHIPPING_PENDING',
            to: 'COMPLETED',
            type: 'order.completed',
            patch,
          },
        ];
      }
      return [];
    }

    case 'shipping.failed': {
      const reason = stringField(event.payload, 'reason') ?? 'SHIPPING_FAILED';
      if (status === 'SHIPPING_PENDING') {
        return [
          {
            kind: 'transition',
            from: 'SHIPPING_PENDING',
            to: 'SHIPPING_FAILED',
            type: 'order.shipping_failed',
            patch: { failureReason: reason },
          },
          { kind: 'publish', type: 'payment.refund_requested' },
        ];
      }
      // Redelivery after the transition but before the refund request was sent.
      if (status === 'SHIPPING_FAILED') {
        return [{ kind: 'publish', type: 'payment.refund_requested' }];
      }
      return [];
    }

    case 'payment.refunded': {
      if (status === 'SHIPPING_FAILED') {
        return [
          {
            kind: 'transition',
            from: 'SHIPPING_FAILED',
            to: 'REFUNDED',
            type: 'order.refunded',
          },
          { kind: 'release', reason: 'REFUNDED' },
        ];
      }
      // Redelivery after the refund commit but before the release.
      if (status === 'REFUNDED') return [{ kind: 'release', reason: 'REFUNDED' }];
      return [];
    }
  }
}

export function makeEnvelope<T extends Record<string, unknown>>(
  orderId: string,
  type: SagaMessageType,
  payload: T = {} as T,
  attempt = 1,
): SagaEnvelope<T> {
  return {
    messageId: randomUUID(),
    orderId,
    type,
    attempt,
    occurredAt: new Date().toISOString(),
    payload,
  };
}

export function isSagaEnvelope(value: unknown): value is SagaEnvelope {
  if (typeof value !== 'object' || value === null) return false;
  const body = value as Partial<SagaEnvelope>;
  return (
    typeof body.messageId === 'string' &&
    body.messageId.length > 0 &&
    typeof body.orderId === 'string' &&
    body.orderId.length > 0 &&
    typeof body.type === 'string' &&
    (SAGA_MESSAGE_TYPES as readonly string[]).includes(body.type) &&
    typeof body.attempt === 'number' &&
    typeof body.occurredAt === 'string' &&
    typeof body.payload === 'object' &&
    body.payload !== null
  );
}

export function isSagaResultEvent(value: unknown): value is SagaEventType {
  return (
    typeof value === 'string' && (SAGA_RESULT_EVENT_TYPES as readonly string[]).includes(value)
  );
}

function stringField(
  payload: Record<string, unknown> | undefined,
  key: string,
): string | undefined {
  const value = payload?.[key];
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function referencePatch(
  field: 'paymentRef' | 'shipmentRef',
  payload: Record<string, unknown> | undefined,
): OrderPatch | undefined {
  const value = stringField(payload, field);
  return value ? { [field]: value } : undefined;
}
