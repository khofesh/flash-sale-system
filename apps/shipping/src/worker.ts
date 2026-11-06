import { setTimeout as sleep } from 'node:timers/promises';
import { deterministicFailure, isSagaEnvelope, makeResultEnvelope, mockReference } from '@fss/core';
import type { Logger, MessageBus, StopFn } from '@fss/infra';

export interface ShippingDeps {
  bus: MessageBus;
  logger: Logger;
  latencyMs?: number;
  failureRate?: number;
  concurrency?: number;
}

/**
 * Mock shipping worker. Like payment, the outcome and the result's message id are
 * derived from the order id, so shipping the same order twice always returns the
 * same result.
 */
export async function startShipping({
  bus,
  logger,
  latencyMs = 100,
  failureRate = 0.02,
  concurrency = 5,
}: ShippingDeps): Promise<StopFn> {
  return bus.subscribe(
    'shipping-requests',
    async (message) => {
      const body = message.body;
      if (!isSagaEnvelope(body) || body.type !== 'shipping.requested') {
        logger.warn({ messageId: message.id }, 'ignoring unsupported shipping request');
        return;
      }

      await sleep(latencyMs);
      const failed = deterministicFailure(body.orderId, failureRate, 'shipping');

      const result = failed
        ? makeResultEnvelope(body.orderId, 'shipping.failed', { reason: 'SHIPPING_UNAVAILABLE' })
        : makeResultEnvelope(body.orderId, 'shipping.succeeded', {
            shipmentRef: mockReference('shp', body.orderId),
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
