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
import { createSqsClient } from '@fss/infra';

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
