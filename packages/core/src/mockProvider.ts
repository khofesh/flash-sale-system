import { createHash } from 'node:crypto';

/**
 * Outcome of a mock provider call, derived from the order id. Calling it again
 * for the same order gives the same answer, so the mocks are idempotent without
 * keeping state. `salt` keeps payment and shipping outcomes independent.
 */
export function deterministicFailure(orderId: string, failureRate: number, salt: string): boolean {
  if (!Number.isFinite(failureRate) || failureRate <= 0) return false;
  if (failureRate >= 1) return true;

  const digest = createHash('sha256').update(`${salt}:${orderId}`).digest();
  return digest.readUInt32BE(0) / 0x1_0000_0000 < failureRate;
}

export function mockReference(prefix: 'pay' | 'shp', orderId: string): string {
  return `${prefix}_${orderId.replaceAll('-', '').slice(0, 16)}`;
}
