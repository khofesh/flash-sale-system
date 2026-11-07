export type QueueName = 'payment-requests' | 'shipping-requests' | 'order-events';

export interface BusMessage {
  id: string;
  queue: QueueName;
  body: unknown;
}

export type MessageHandler = (message: BusMessage) => Promise<void>;

export interface SubscribeOptions {
  concurrency?: number;
  /**
   * Checked before each receive. While it is false (or rejects) nothing is
   * received, so an outage of a dependency the handler needs does not use up the
   * message's receives and send it to the DLQ.
   */
  canReceive?: () => Promise<boolean>;
}

export type StopFn = () => Promise<void>;

export interface MessageBus {
  publish(queue: QueueName, body: unknown): Promise<void>;
  subscribe(queue: QueueName, handler: MessageHandler, options?: SubscribeOptions): Promise<StopFn>;
}
