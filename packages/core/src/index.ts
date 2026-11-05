export * from './sale.js';

export interface HelloHop {
  service: string;
  hostname: string;
  at: string;
}

export type HelloStatus = 'QUEUED' | 'PAID' | 'DONE';

export const HELLO_STREAM = 'hello:stream';
export const HELLO_GROUP = 'orchestrator';

export interface HelloRecord {
  id: string;
  name: string;
  hops: HelloHop[];
  status: HelloStatus;
  createdAt: string;
  updatedAt: string;
}

export interface HelloRequestMessage {
  messageId: string;
  type: 'hello.payment' | 'hello.ship';
  helloId: string;
  name: string;
}

export interface HelloResultMessage {
  messageId: string;
  type: 'hello.paid' | 'hello.shipped';
  helloId: string;
  name: string;
  hop: HelloHop;
}

export function hello(name: string): string {
  return `Hello, ${name}!`;
}

export function makeHop(service: string, hostname: string, at = new Date()): HelloHop {
  return { service, hostname, at: at.toISOString() };
}

export function isHelloRequest(body: unknown): body is HelloRequestMessage {
  if (typeof body !== 'object' || body === null) return false;
  const c = body as Partial<HelloRequestMessage>;
  return (
    (c.type === 'hello.payment' || c.type === 'hello.ship') &&
    typeof c.messageId === 'string' &&
    typeof c.helloId === 'string' &&
    typeof c.name === 'string'
  );
}

export function isHelloResult(body: unknown): body is HelloResultMessage {
  if (typeof body !== 'object' || body === null) return false;
  const c = body as Partial<HelloResultMessage>;
  return (
    (c.type === 'hello.paid' || c.type === 'hello.shipped') &&
    typeof c.messageId === 'string' &&
    typeof c.helloId === 'string' &&
    typeof c.name === 'string' &&
    typeof c.hop === 'object' &&
    c.hop !== null &&
    typeof c.hop.service === 'string' &&
    typeof c.hop.hostname === 'string' &&
    typeof c.hop.at === 'string'
  );
}
