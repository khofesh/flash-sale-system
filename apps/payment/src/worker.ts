import { createHash } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { isSagaEnvelope, makeEnvelope, type SagaEnvelope } from '@fss/core';
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
 * Mock payment worker. The outcome is derived deterministically from the order
 * id, so a redelivered `payment.requested` always charges the same order with
 * the same result: the mock is idempotent on `orderId` without keeping state.
 */
export async function startPayment({
  bus,
  logger,
  latencyMs = 200,
  refundLatencyMs = 50,
  failureRate = 0.1,
  concurrency = 10,
}: PaymentDeps): Promise<StopFn> {
  const rate = clampRate(failureRate);

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
        const failed = deterministicFailure(body.orderId, rate, 'payment');
        const result: SagaEnvelope = failed
          ? makeEnvelope(body.orderId, 'payment.failed', { reason: 'PAYMENT_DECLINED' })
          : makeEnvelope(body.orderId, 'payment.succeeded', {
              paymentRef: reference('pay', body.orderId),
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
        const result = makeEnvelope(body.orderId, 'payment.refunded', {
          paymentRef: paymentReference(body.orderId),
        });
        await bus.publish('order-events', result);
        logger.info({ orderId: body.orderId }, 'mock payment refunded');
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

function deterministicFailure(orderId: string, failureRate: number, salt: string): boolean {
  if (failureRate <= 0) return false;
  if (failureRate >= 1) return true;

  const digest = createHash('sha256').update(`${salt}:${orderId}`).digest();
  const unit = digest.readUInt32BE(0) / 0x1_0000_0000;
  return unit < failureRate;
}

function reference(prefix: string, orderId: string): string {
  return `${prefix}_${orderId.replaceAll('-', '').slice(0, 16)}`;
}

function paymentReference(orderId: string): string {
  return reference('pay', orderId);
}

function clampRate(rate: number): number {
  return Number.isFinite(rate) ? Math.min(1, Math.max(0, rate)) : 0;
}
