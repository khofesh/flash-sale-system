import {
  DeleteMessageCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { setTimeout as sleep } from 'node:timers/promises';
import type { Logger } from '../logger.js';
import type {
  BusMessage,
  MessageBus,
  MessageHandler,
  QueueName,
  StopFn,
  SubscribeOptions,
} from './types.js';

export interface SqsBusOptions {
  endpoint: string;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
}

export function createSqsClient(options: SqsBusOptions): SQSClient {
  return new SQSClient({
    endpoint: options.endpoint,
    region: options.region,
    credentials: {
      accessKeyId: options.accessKeyId,
      secretAccessKey: options.secretAccessKey,
    },
  });
}

export class SqsBus implements MessageBus {
  private readonly queueUrls = new Map<QueueName, Promise<string>>();

  constructor(
    private readonly client: SQSClient,
    private readonly logger: Logger,
  ) {}

  async publish(queue: QueueName, body: unknown): Promise<void> {
    const queueUrl = await this.queueUrl(queue);
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(body),
      }),
    );
  }

  async subscribe(
    queue: QueueName,
    handler: MessageHandler,
    options?: SubscribeOptions,
  ): Promise<StopFn> {
    const controller = new AbortController();
    const concurrency = Math.max(1, options?.concurrency ?? 1);
    const loops = Array.from({ length: concurrency }, () =>
      this.consume(queue, handler, controller.signal, options?.canReceive),
    );

    return async () => {
      controller.abort();
      await Promise.allSettled(loops);
    };
  }

  private queueUrl(queue: QueueName): Promise<string> {
    const cached = this.queueUrls.get(queue);
    if (cached) return cached;

    const pending = this.client
      .send(new GetQueueUrlCommand({ QueueName: queue }))
      .then((response) => {
        if (!response.QueueUrl) {
          throw new Error(`SQS queue ${queue} has no URL`);
        }
        return response.QueueUrl;
      });

    this.queueUrls.set(queue, pending);
    pending.catch(() => this.queueUrls.delete(queue));
    return pending;
  }

  private async consume(
    queue: QueueName,
    handler: MessageHandler,
    signal: AbortSignal,
    canReceive?: () => Promise<boolean>,
  ): Promise<void> {
    let queueUrl: string | undefined;

    while (!signal.aborted) {
      if (canReceive && !(await canReceive().catch(() => false))) {
        await sleep(1_000);
        continue;
      }

      let messages;
      try {
        queueUrl ??= await this.queueUrl(queue);
        const response = await this.client.send(
          new ReceiveMessageCommand({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: 1,
            VisibilityTimeout: 30,
          }),
          { abortSignal: signal },
        );
        messages = response.Messages ?? [];
      } catch (error) {
        if (signal.aborted) return;
        this.logger.error({ err: error, queue }, 'sqs receive failed');
        await sleep(1_000);
        continue;
      }

      for (const message of messages) {
        if (signal.aborted) return;

        let body: unknown;
        try {
          body = JSON.parse(message.Body ?? '');
        } catch (error) {
          this.logger.error(
            { err: error, queue, messageId: message.MessageId },
            'invalid sqs message body',
          );
          continue;
        }

        const received: BusMessage = {
          id: message.MessageId ?? '',
          queue,
          body,
        };

        try {
          await handler(received);
          // Not tied to the shutdown signal: once the handler has succeeded the
          // delete must go through, or the message is redelivered.
          await this.client.send(
            new DeleteMessageCommand({
              QueueUrl: queueUrl,
              ReceiptHandle: message.ReceiptHandle,
            }),
          );
        } catch (error) {
          this.logger.error(
            { err: error, queue, messageId: message.MessageId },
            'message handling failed; leaving it for redelivery',
          );
          // An outage: the rest of the batch would only fail slowly too, keeping
          // this loop from reaching the check above for minutes.
          if (canReceive && !(await canReceive().catch(() => false))) break;
        }
      }
    }
  }
}
