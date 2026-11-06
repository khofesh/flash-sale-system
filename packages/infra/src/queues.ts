import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  SetQueueAttributesCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { QueueName } from './bus/types.js';

interface QueueDefinition {
  name: QueueName;
  deadLetterQueue: `${QueueName}-dlq`;
}

const QUEUE_DEFINITIONS: readonly QueueDefinition[] = [
  { name: 'payment-requests', deadLetterQueue: 'payment-requests-dlq' },
  { name: 'shipping-requests', deadLetterQueue: 'shipping-requests-dlq' },
  { name: 'order-events', deadLetterQueue: 'order-events-dlq' },
];

const VISIBILITY_TIMEOUT_SECONDS = '30';
const MAX_RECEIVE_COUNT = '5';

export interface InitQueuesResult {
  name: string;
  url: string;
  deadLetterQueueUrl: string;
}

export async function initQueues(client: SQSClient): Promise<InitQueuesResult[]> {
  const results: InitQueuesResult[] = [];

  for (const definition of QUEUE_DEFINITIONS) {
    const deadLetterQueueUrl = await ensureQueue(client, definition.deadLetterQueue, {
      VisibilityTimeout: VISIBILITY_TIMEOUT_SECONDS,
    });
    const deadLetterQueueArn = await queueArn(client, deadLetterQueueUrl);

    const url = await ensureQueue(client, definition.name, {
      VisibilityTimeout: VISIBILITY_TIMEOUT_SECONDS,
      RedrivePolicy: JSON.stringify({
        deadLetterTargetArn: deadLetterQueueArn,
        maxReceiveCount: MAX_RECEIVE_COUNT,
      }),
    });

    results.push({ name: definition.name, url, deadLetterQueueUrl });
  }

  return results;
}

async function ensureQueue(
  client: SQSClient,
  name: string,
  attributes: Record<string, string>,
): Promise<string> {
  try {
    const url = await getQueueUrl(client, name);
    await client.send(new SetQueueAttributesCommand({ QueueUrl: url, Attributes: attributes }));
    return url;
  } catch (error) {
    if (!isMissingQueue(error)) throw error;
  }

  const created = await client.send(
    new CreateQueueCommand({ QueueName: name, Attributes: attributes }),
  );
  if (!created.QueueUrl) {
    throw new Error(`SQS did not return a URL for queue ${name}`);
  }
  return created.QueueUrl;
}

async function getQueueUrl(client: SQSClient, name: string): Promise<string> {
  const response = await client.send(new GetQueueUrlCommand({ QueueName: name }));
  if (!response.QueueUrl) {
    throw new Error(`SQS did not return a URL for queue ${name}`);
  }
  return response.QueueUrl;
}

/** Visible messages per dead-letter queue, keyed by queue name. */
export async function readDeadLetterDepths(client: SQSClient): Promise<Record<string, number>> {
  const depths: Record<string, number> = {};

  for (const definition of QUEUE_DEFINITIONS) {
    let queueUrl: string | undefined;
    try {
      queueUrl = await getQueueUrl(client, definition.deadLetterQueue);
    } catch (error) {
      if (isMissingQueue(error)) continue;
      throw error;
    }

    const attributes = await client.send(
      new GetQueueAttributesCommand({
        QueueUrl: queueUrl,
        AttributeNames: ['ApproximateNumberOfMessages'],
      }),
    );
    depths[definition.deadLetterQueue] = Number(
      attributes.Attributes?.ApproximateNumberOfMessages ?? 0,
    );
  }

  return depths;
}

/** Total visible messages across every dead-letter queue. */
export async function countDeadLetters(client: SQSClient): Promise<number> {
  const depths = await readDeadLetterDepths(client);
  return Object.values(depths).reduce((total, depth) => total + depth, 0);
}

async function queueArn(client: SQSClient, queueUrl: string): Promise<string> {
  const response = await client.send(
    new GetQueueAttributesCommand({ QueueUrl: queueUrl, AttributeNames: ['QueueArn'] }),
  );
  const arn = response.Attributes?.QueueArn;
  if (!arn) {
    throw new Error(`SQS did not return an ARN for ${queueUrl}`);
  }
  return arn;
}

function isMissingQueue(error: unknown): boolean {
  if (typeof error !== 'object' || error === null || !('name' in error)) return false;
  const name = (error as { name?: unknown }).name;
  return (
    name === 'QueueDoesNotExist' ||
    name === 'AWS.SimpleQueueService.NonExistentQueue' ||
    name === 'QueueDoesNotExistException'
  );
}
