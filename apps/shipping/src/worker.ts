import { randomUUID } from 'node:crypto';
import os from 'node:os';
import { makeHop, type HelloRequestMessage, type HelloResultMessage } from '@fss/core';
import type { Logger, MessageBus, StopFn } from '@fss/infra';

export interface ShippingDeps {
  bus: MessageBus;
  logger: Logger;
}

export async function startShipping({ bus, logger }: ShippingDeps): Promise<StopFn> {
  return bus.subscribe('shipping-requests', async (message) => {
    const body = message.body;
    if (!isHelloRequest(body) || body.type !== 'hello.ship') {
      logger.warn({ body }, 'ignoring unknown shipping request');
      return;
    }

    const result: HelloResultMessage = {
      messageId: randomUUID(),
      type: 'hello.shipped',
      helloId: body.helloId,
      name: body.name,
      hop: makeHop('shipping', os.hostname()),
    };

    await bus.publish('order-events', result);
    logger.info({ helloId: body.helloId }, 'hello shipment completed');
  });
}

function isHelloRequest(body: unknown): body is HelloRequestMessage {
  if (typeof body !== 'object' || body === null) return false;
  const candidate = body as Partial<HelloRequestMessage>;
  return (
    typeof candidate.type === 'string' &&
    typeof candidate.helloId === 'string' &&
    typeof candidate.name === 'string'
  );
}
