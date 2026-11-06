import { setTimeout as sleep } from 'node:timers/promises';
import {
  deterministicFailure,
  isSagaEnvelope,
  makeResultEnvelope,
  mockReference,
  type SagaEnvelope,
} from '@fss/core';
import type { Logger, MessageBus, StopFn } from '@fss/infra';

export interface PaymentDeps {
  bus: MessageBus;
  logger: Logger;
  latencyMs?: number;
  refundLatencyMs?: number;
  failureRate?: number;
  concurrency?: number;
}

/**
 * Mock payment worker. The outcome and the result's message id are derived from
 * the order id, so a redelivered `payment.requested` always charges the same
 * order with the same result: the mock is idempotent on `orderId` without
 * keeping state.
 */
export async function startPayment({
  bus,
  logger,
  latencyMs = 200,
  refundLatencyMs = 50,
  failureRate = 0.1,
  concurrency = 10,
}: PaymentDeps): Promise<StopFn> {
  return bus.subscribe(
    'payment-requests',
    async (message) => {
      const body = message.body;
      if (!isSagaEnvelope(body)) {
        logger.warn({ messageId: message.id }, 'ignoring malformed payment request');
        return;
      }

      if (body.type === 'payment.requested') {
        await sleep(latencyMs);
        const failed = deterministicFailure(body.orderId, failureRate, 'payment');
        const result: SagaEnvelope = failed
          ? makeResultEnvelope(body.orderId, 'payment.failed', { reason: 'PAYMENT_DECLINED' })
          : makeResultEnvelope(body.orderId, 'payment.succeeded', {
              paymentRef: mockReference('pay', body.orderId),
            });

        await bus.publish('order-events', result);
        logger.info(
          { orderId: body.orderId, outcome: result.type },
          failed ? 'mock payment declined' : 'mock payment captured',
        );
        return;
      }

      if (body.type === 'payment.refund_requested') {
        await sleep(refundLatencyMs);
        const { paymentRef } = body.payload;
        const result = makeResultEnvelope(body.orderId, 'payment.refunded', {
          paymentRef: typeof paymentRef === 'string' ? paymentRef : null,
        });
        await bus.publish('order-events', result);
        logger.info({ orderId: body.orderId, paymentRef }, 'mock payment refunded');
        return;
      }

      logger.warn(
        { messageId: message.id, type: body.type },
        'ignoring unsupported payment request type',
      );
    },
    { concurrency },
  );
}
