import { describe, expect, it } from 'vitest';
import type { OrderStatus } from './purchase.js';
import {
  ORDER_TRANSITIONS,
  SAGA_RESULT_EVENT_TYPES,
  isSagaEnvelope,
  makeEnvelope,
  makeResultEnvelope,
  nextAction,
  transitionTarget,
  type SagaEvent,
  type SagaStep,
} from './orderStateMachine.js';

function transitionSteps(steps: SagaStep[]): Array<[OrderStatus, OrderStatus]> {
  return steps.flatMap((step) =>
    step.kind === 'transition' ? [[step.from, step.to] as [OrderStatus, OrderStatus]] : [],
  );
}

function published(steps: SagaStep[]): string[] {
  return steps.flatMap((step) => (step.kind === 'publish' ? [step.type] : []));
}

function releases(steps: SagaStep[]): string[] {
  return steps.flatMap((step) => (step.kind === 'release' ? [step.reason] : []));
}

describe('order state machine', () => {
  it('starts payment after a reservation is persisted', () => {
    const steps = nextAction({ status: 'RESERVED' }, { type: 'reservation.persisted' });
    expect(transitionSteps(steps)).toEqual([['RESERVED', 'PAYMENT_PENDING']]);
    expect(published(steps)).toEqual(['payment.requested']);
  });

  it('republishes payment.requested when a reservation is redelivered', () => {
    const steps = nextAction({ status: 'PAYMENT_PENDING' }, { type: 'reservation.persisted' });
    expect(transitionSteps(steps)).toEqual([]);
    expect(published(steps)).toEqual(['payment.requested']);
  });

  it('moves payment success through PAID and SHIPPING_PENDING', () => {
    const event: SagaEvent = {
      type: 'payment.succeeded',
      payload: { paymentRef: 'pay_1' },
    };
    const steps = nextAction({ status: 'PAYMENT_PENDING' }, event);

    expect(transitionSteps(steps)).toEqual([
      ['PAYMENT_PENDING', 'PAID'],
      ['PAID', 'SHIPPING_PENDING'],
    ]);
    expect(published(steps)).toEqual(['shipping.requested']);
    expect(steps[0]).toMatchObject({ patch: { paymentRef: 'pay_1' } });
  });

  it('repairs a crash between PAID and SHIPPING_PENDING', () => {
    const event: SagaEvent = { type: 'payment.succeeded', payload: { paymentRef: 'pay_1' } };
    const steps = nextAction({ status: 'PAID' }, event);
    expect(transitionSteps(steps)).toEqual([['PAID', 'SHIPPING_PENDING']]);
    expect(published(steps)).toEqual(['shipping.requested']);
  });

  it('republishes shipping.requested while the order is already shipping', () => {
    const steps = nextAction({ status: 'SHIPPING_PENDING' }, { type: 'payment.succeeded' });
    expect(transitionSteps(steps)).toEqual([]);
    expect(published(steps)).toEqual(['shipping.requested']);
  });

  it('cancels and releases stock when payment fails', () => {
    const steps = nextAction(
      { status: 'PAYMENT_PENDING' },
      { type: 'payment.failed', payload: { reason: 'CARD_DECLINED' } },
    );
    expect(transitionSteps(steps)).toEqual([['PAYMENT_PENDING', 'CANCELLED']]);
    expect(releases(steps)).toEqual(['CARD_DECLINED']);
  });

  it('cancels and releases stock when payment times out', () => {
    const steps = nextAction({ status: 'PAYMENT_PENDING' }, { type: 'payment.timeout' });
    expect(transitionSteps(steps)).toEqual([['PAYMENT_PENDING', 'CANCELLED']]);
    expect(releases(steps)).toEqual(['PAYMENT_TIMEOUT']);
  });

  it('releases again when a cancellation event is redelivered', () => {
    const steps = nextAction({ status: 'CANCELLED' }, { type: 'payment.failed' });
    expect(transitionSteps(steps)).toEqual([]);
    expect(releases(steps)).toEqual(['PAYMENT_DECLINED']);
  });

  it('records the charge and refunds a late payment success after cancellation', () => {
    const steps = nextAction(
      { status: 'CANCELLED' },
      { type: 'payment.succeeded', payload: { paymentRef: 'pay_late' } },
    );
    expect(transitionSteps(steps)).toEqual([['CANCELLED', 'CANCELLED']]);
    expect(steps[0]).toMatchObject({ patch: { paymentRef: 'pay_late' } });
    expect(published(steps)).toEqual(['payment.refund_requested']);
  });

  it('closes a cancelled order when its late charge is refunded', () => {
    const steps = nextAction({ status: 'CANCELLED' }, { type: 'payment.refunded' });
    expect(transitionSteps(steps)).toEqual([['CANCELLED', 'REFUNDED']]);
    expect(releases(steps)).toEqual(['REFUNDED']);
  });

  it('completes an order when shipping succeeds', () => {
    const steps = nextAction(
      { status: 'SHIPPING_PENDING' },
      { type: 'shipping.succeeded', payload: { shipmentRef: 'shp_1' } },
    );
    expect(transitionSteps(steps)).toEqual([['SHIPPING_PENDING', 'COMPLETED']]);
    expect(steps[0]).toMatchObject({ patch: { shipmentRef: 'shp_1' } });
  });

  it('starts a refund when shipping fails', () => {
    const steps = nextAction(
      { status: 'SHIPPING_PENDING' },
      { type: 'shipping.failed', payload: { reason: 'COURIER_DOWN' } },
    );
    expect(transitionSteps(steps)).toEqual([['SHIPPING_PENDING', 'SHIPPING_FAILED']]);
    expect(published(steps)).toEqual(['payment.refund_requested']);
  });

  it('republishes the refund request when shipping failure is redelivered', () => {
    const steps = nextAction({ status: 'SHIPPING_FAILED' }, { type: 'shipping.failed' });
    expect(transitionSteps(steps)).toEqual([]);
    expect(published(steps)).toEqual(['payment.refund_requested']);
  });

  it('refunds and releases stock after shipping failure', () => {
    const steps = nextAction({ status: 'SHIPPING_FAILED' }, { type: 'payment.refunded' });
    expect(transitionSteps(steps)).toEqual([['SHIPPING_FAILED', 'REFUNDED']]);
    expect(releases(steps)).toEqual(['REFUNDED']);
  });

  it('releases again when a refund is redelivered', () => {
    const steps = nextAction({ status: 'REFUNDED' }, { type: 'payment.refunded' });
    expect(transitionSteps(steps)).toEqual([]);
    expect(releases(steps)).toEqual(['REFUNDED']);
  });

  it('ignores events that do not apply to the current status', () => {
    expect(nextAction({ status: 'RESERVED' }, { type: 'shipping.succeeded' })).toEqual([]);
    expect(nextAction({ status: 'COMPLETED' }, { type: 'payment.failed' })).toEqual([]);
    expect(nextAction({ status: 'REFUNDED' }, { type: 'shipping.succeeded' })).toEqual([]);
    expect(nextAction({ status: 'SHIPPING_FAILED' }, { type: 'payment.succeeded' })).toEqual([]);
  });

  it('exposes the legal transitions table', () => {
    expect(transitionTarget('RESERVED', 'reservation.persisted')).toBe('PAYMENT_PENDING');
    expect(transitionTarget('PAYMENT_PENDING', 'payment.failed')).toBe('CANCELLED');
    expect(transitionTarget('SHIPPING_PENDING', 'shipping.failed')).toBe('SHIPPING_FAILED');
    expect(transitionTarget('SHIPPING_FAILED', 'payment.refunded')).toBe('REFUNDED');
    expect(transitionTarget('COMPLETED', 'payment.succeeded')).toBeUndefined();
    expect(ORDER_TRANSITIONS.COMPLETED).toEqual({});
    expect(ORDER_TRANSITIONS.REFUNDED).toEqual({});
  });

  it('only plans transitions the table allows', () => {
    const events = [
      ...SAGA_RESULT_EVENT_TYPES,
      'reservation.persisted',
      'payment.timeout',
    ] as const;
    for (const status of Object.keys(ORDER_TRANSITIONS) as OrderStatus[]) {
      for (const type of events) {
        for (const [from, to] of transitionSteps(nextAction({ status }, { type }))) {
          expect(transitionTarget(from, type)).toBe(to);
        }
      }
    }
  });
});

describe('saga envelopes', () => {
  it('creates a valid envelope', () => {
    const envelope = makeEnvelope('order-1', 'payment.requested', { saleId: 'sale-1' });
    expect(isSagaEnvelope(envelope)).toBe(true);
    expect(envelope).toMatchObject({
      orderId: 'order-1',
      type: 'payment.requested',
      attempt: 1,
      payload: { saleId: 'sale-1' },
    });
    expect(Number.isNaN(Date.parse(envelope.occurredAt))).toBe(false);
  });

  it('gives a redelivered result the same message id', () => {
    const first = makeResultEnvelope('order-1', 'payment.failed', { reason: 'DECLINED' });
    const second = makeResultEnvelope('order-1', 'payment.failed', { reason: 'DECLINED' });
    expect(isSagaEnvelope(first)).toBe(true);
    expect(second.messageId).toBe(first.messageId);
    expect(makeResultEnvelope('order-1', 'payment.refunded').messageId).not.toBe(first.messageId);
    expect(makeResultEnvelope('order-2', 'payment.failed').messageId).not.toBe(first.messageId);
  });

  it('rejects malformed envelopes', () => {
    expect(isSagaEnvelope(null)).toBe(false);
    expect(isSagaEnvelope({})).toBe(false);
    expect(
      isSagaEnvelope({
        messageId: 'm',
        orderId: 'o',
        type: 'not-a-saga-message',
        attempt: 1,
        occurredAt: new Date().toISOString(),
        payload: {},
      }),
    ).toBe(false);
  });
});
