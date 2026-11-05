export const QUEUE_NAMES = ['payment-requests', 'shipping-requests', 'order-events'] as const;

export type QueueName = (typeof QUEUE_NAMES)[number];

export interface BusMessage {
  id: string;
  queue: QueueName;
  body: unknown;
}

export type MessageHandler = (message: BusMessage) => Promise<void>;

export interface SubscribeOptions {
  concurrency?: number;
}

export type StopFn = () => Promise<void>;

export interface MessageBus {
  publish(queue: QueueName, body: unknown): Promise<void>;
  subscribe(queue: QueueName, handler: MessageHandler, options?: SubscribeOptions): Promise<StopFn>;
}
