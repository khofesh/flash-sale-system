import os from 'node:os';
import { isHelloRequest, makeHop, type HelloResultMessage } from '@fss/core';
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
      messageId: `${body.messageId}:shipped`,
      type: 'hello.shipped',
      helloId: body.helloId,
      name: body.name,
      hop: makeHop('shipping', os.hostname()),
    };

    await bus.publish('order-events', result);
    logger.info({ helloId: body.helloId }, 'hello shipment completed');
  });
}
