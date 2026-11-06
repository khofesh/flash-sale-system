import { createHash } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { isSagaEnvelope, makeEnvelope } from '@fss/core';
import type { Logger, MessageBus, StopFn } from '@fss/infra';

export interface ShippingDeps {
  bus: MessageBus;
  logger: Logger;
  latencyMs?: number;
  failureRate?: number;
  concurrency?: number;
}

/**
 * Mock shipping worker. Like payment, the outcome is deterministic per order id,
 * so shipping the same order twice always returns the same result.
 */
export async function startShipping({
  bus,
  logger,
  latencyMs = 100,
  failureRate = 0.02,
  concurrency = 5,
}: ShippingDeps): Promise<StopFn> {
  const rate = clampRate(failureRate);

  return bus.subscribe(
    'shipping-requests',
    async (message) => {
      const body = message.body;
      if (!isSagaEnvelope(body) || body.type !== 'shipping.requested') {
        logger.warn({ messageId: message.id }, 'ignoring unsupported shipping request');
        return;
      }

      await sleep(latencyMs);
      const failed = deterministicFailure(body.orderId, rate);

      const result = failed
        ? makeEnvelope(body.orderId, 'shipping.failed', { reason: 'SHIPPING_UNAVAILABLE' })
        : makeEnvelope(body.orderId, 'shipping.succeeded', {
            shipmentRef: `shp_${body.orderId.replaceAll('-', '').slice(0, 16)}`,
            carrier: 'mock-express',
          });

      await bus.publish('order-events', result);
      logger.info(
        { orderId: body.orderId, outcome: result.type },
        failed ? 'mock shipment failed' : 'mock shipment created',
      );
    },
    { concurrency },
  );
}

function deterministicFailure(orderId: string, failureRate: number): boolean {
  if (failureRate <= 0) return false;
  if (failureRate >= 1) return true;

  const digest = createHash('sha256').update(`shipping:${orderId}`).digest();
  const unit = digest.readUInt32BE(0) / 0x1_0000_0000;
  return unit < failureRate;
}

function clampRate(rate: number): number {
  return Number.isFinite(rate) ? Math.min(1, Math.max(0, rate)) : 0;
}
