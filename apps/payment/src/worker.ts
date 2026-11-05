import { randomUUID } from 'node:crypto';
import os from 'node:os';
import { makeHop, type HelloRequestMessage, type HelloResultMessage } from '@fss/core';
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
        messageId: randomUUID(),
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

function isHelloRequest(body: unknown): body is HelloRequestMessage {
  if (typeof body !== 'object' || body === null) return false;
  const candidate = body as Partial<HelloRequestMessage>;
  return (
    typeof candidate.type === 'string' &&
    typeof candidate.helloId === 'string' &&
    typeof candidate.name === 'string'
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
