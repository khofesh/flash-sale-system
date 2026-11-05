import os from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import { isHelloRequest, makeHop, type HelloResultMessage } from '@fss/core';
import type { Logger, MessageBus, StopFn } from '@fss/infra';

export interface PaymentDeps {
  bus: MessageBus;
  logger: Logger;
  latencyMs?: number;
  concurrency?: number;
}

export async function startPayment({
  bus,
  logger,
  latencyMs = 100,
  concurrency = 10,
}: PaymentDeps): Promise<StopFn> {
  return bus.subscribe(
    'payment-requests',
    async (message) => {
      const body = message.body;
      if (!isHelloRequest(body) || body.type !== 'hello.payment') {
        logger.warn({ body }, 'ignoring unknown payment request');
        return;
      }

      await sleep(latencyMs);

      const result: HelloResultMessage = {
        messageId: `${body.messageId}:paid`,
        type: 'hello.paid',
        helloId: body.helloId,
        name: body.name,
        hop: makeHop('payment', os.hostname()),
      };

      await bus.publish('order-events', result);
      logger.info({ helloId: body.helloId }, 'hello payment completed');
    },
    { concurrency },
  );
}
