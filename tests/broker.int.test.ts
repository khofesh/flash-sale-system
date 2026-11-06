import { setTimeout as sleep } from 'node:timers/promises';
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  CreateQueueCommand,
  DeleteMessageBatchCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageBatchCommand,
  SendMessageCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { GenericContainer, type StartedTestContainer } from 'testcontainers';
import { SqsBus, createLogger, createSqsClient } from '@fss/infra';

const flociImage = 'floci/floci:2.4.0';

describe('SQS broker contract (Floci)', () => {
  let container: StartedTestContainer;
  let client: SQSClient;

  beforeAll(async () => {
    container = await new GenericContainer(flociImage).withExposedPorts(4566).start();
    client = createSqsClient({
      endpoint: `http://${container.getHost()}:${container.getMappedPort(4566)}`,
      region: 'us-east-1',
      accessKeyId: 'test',
      secretAccessKey: 'test',
    });
  }, 180_000);

  afterAll(async () => {
    client?.destroy();
    await container?.stop();
  });

  function name(label: string): string {
    return `${label}-${randomUUID().slice(0, 8)}`;
  }

  async function createQueue(queue: string, attributes: Record<string, string> = {}) {
    const response = await client.send(
      new CreateQueueCommand({ QueueName: queue, Attributes: attributes }),
    );
    if (!response.QueueUrl) throw new Error(`No URL for ${queue}`);
    return response.QueueUrl;
  }

  it('leaves messages unreceived while the subscriber cannot handle them', async () => {
    const queueUrl = await createQueue('order-events');
    const bus = new SqsBus(client, createLogger('broker-contract', 'silent'));
    const received: unknown[] = [];
    let open = false;

    const stop = await bus.subscribe(
      'order-events',
      async (message) => {
        received.push(message.body);
      },
      { canReceive: async () => open },
    );

    try {
      await bus.publish('order-events', { id: 1 });
      await sleep(2_500);

      // Still waiting, and never received, so it has used none of its receives.
      const attributes = await client.send(
        new GetQueueAttributesCommand({
          QueueUrl: queueUrl,
          AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
        }),
      );
      expect(attributes.Attributes?.ApproximateNumberOfMessages).toBe('1');
      expect(attributes.Attributes?.ApproximateNumberOfMessagesNotVisible).toBe('0');
      expect(received).toHaveLength(0);

      open = true;
      const deadline = Date.now() + 10_000;
      while (received.length === 0 && Date.now() < deadline) await sleep(50);
      expect(received).toEqual([{ id: 1 }]);
    } finally {
      await stop();
    }
  });

  it('abandons the rest of a batch when a failure turns out to be an outage', async () => {
    await createQueue('shipping-requests');
    const bus = new SqsBus(client, createLogger('broker-contract', 'silent'));
    let attempts = 0;
    let open = true;

    for (const id of [1, 2, 3]) await bus.publish('shipping-requests', { id });
    const stop = await bus.subscribe(
      'shipping-requests',
      async () => {
        attempts += 1;
        open = false;
        throw new Error('postgres is down');
      },
      { canReceive: async () => open },
    );

    try {
      await sleep(2_500);
      expect(attempts).toBe(1);
    } finally {
      await stop();
    }
  });

  async function createWithDlq(visibilityTimeout: string, maxReceiveCount: string) {
    const dlq = await createQueue(name('contract-dlq'));
    const arn = await client
      .send(new GetQueueAttributesCommand({ QueueUrl: dlq, AttributeNames: ['QueueArn'] }))
      .then((response) => response.Attributes?.QueueArn);
    if (!arn) throw new Error('No DLQ ARN');

    const queue = await createQueue(name('contract'), {
      VisibilityTimeout: visibilityTimeout,
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn, maxReceiveCount }),
    });
    return { queue, dlq };
  }

  async function receive(queueUrl: string): Promise<Message[]> {
    const response = await client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 0,
      }),
    );
    return response.Messages ?? [];
  }

  it('redelivers a message that was not deleted after the visibility timeout', async () => {
    const { queue } = await createWithDlq('1', '5');
    await client.send(new SendMessageCommand({ QueueUrl: queue, MessageBody: 'hello' }));

    const first = await receive(queue);
    expect(first).toHaveLength(1);
    expect(first[0]?.Body).toBe('hello');

    // Still invisible.
    expect(await receive(queue)).toHaveLength(0);

    await sleep(1_300);
    const redelivered = await receive(queue);
    expect(redelivered).toHaveLength(1);
    expect(redelivered[0]?.MessageId).toBe(first[0]?.MessageId);

    const receipt = redelivered[0]?.ReceiptHandle;
    if (receipt) {
      await client.send(
        new DeleteMessageBatchCommand({
          QueueUrl: queue,
          Entries: [{ Id: '0', ReceiptHandle: receipt }],
        }),
      );
    }
    await sleep(200);
    expect(await receive(queue)).toHaveLength(0);
  }, 60_000);

  it('moves a poison message to the DLQ after maxReceiveCount receives', async () => {
    const { queue, dlq } = await createWithDlq('1', '2');
    await client.send(new SendMessageCommand({ QueueUrl: queue, MessageBody: 'poison' }));

    for (let attempt = 0; attempt < 10; attempt += 1) {
      await receive(queue);
      await sleep(1_200);
      const dead = await receive(dlq);
      if (dead.length > 0) {
        expect(dead[0]?.Body).toBe('poison');
        return;
      }
    }

    throw new Error('message never reached the dead-letter queue');
  }, 60_000);

  it('supports batch send and batch delete', async () => {
    const queue = await createQueue(name('batch'));
    const bodies = Array.from({ length: 10 }, (_, index) => `batch-${index}`);

    await client.send(
      new SendMessageBatchCommand({
        QueueUrl: queue,
        Entries: bodies.map((body, index) => ({ Id: String(index), MessageBody: body })),
      }),
    );

    const received = await receive(queue);
    expect(received).toHaveLength(10);
    expect(new Set(received.map((message) => message.Body))).toEqual(new Set(bodies));

    await client.send(
      new DeleteMessageBatchCommand({
        QueueUrl: queue,
        Entries: received.flatMap((message, index) =>
          message.ReceiptHandle
            ? [{ Id: String(index), ReceiptHandle: message.ReceiptHandle }]
            : [],
        ),
      }),
    );

    await sleep(200);
    expect(await receive(queue)).toHaveLength(0);
  }, 60_000);
});
