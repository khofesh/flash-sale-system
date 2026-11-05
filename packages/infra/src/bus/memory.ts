import { randomUUID } from 'node:crypto';
import type {
  BusMessage,
  MessageBus,
  MessageHandler,
  QueueName,
  StopFn,
  SubscribeOptions,
} from './types.js';

export class InMemoryBus implements MessageBus {
  private readonly subscribers = new Map<QueueName, Set<MessageHandler>>();

  async publish(queue: QueueName, body: unknown): Promise<void> {
    const handlers = this.subscribers.get(queue);
    if (!handlers) return;
    const message: BusMessage = { id: randomUUID(), queue, body };
    for (const handler of handlers) {
      await handler(message);
    }
  }

  async subscribe(
    queue: QueueName,
    handler: MessageHandler,
    _options?: SubscribeOptions,
  ): Promise<StopFn> {
    let handlers = this.subscribers.get(queue);
    if (!handlers) {
      handlers = new Set();
      this.subscribers.set(queue, handlers);
    }
    handlers.add(handler);
    return async () => {
      handlers.delete(handler);
    };
  }
}
