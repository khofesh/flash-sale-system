export interface HelloHop {
  service: string;
  hostname: string;
  at: string;
}

export type HelloStatus = 'QUEUED' | 'DONE';

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
