import { describe, expect, it, vi } from 'vitest';
import { backoffDelay, retry } from './retry.ts';

describe('backoffDelay', () => {
  it('doubles per attempt up to the cap', () => {
    const options = { baseMs: 100, maxMs: 350, jitter: 0 };
    expect([1, 2, 3, 4].map((attempt) => backoffDelay(attempt, options))).toEqual([
      100, 200, 350, 350,
    ]);
  });

  it('removes at most the jitter fraction', () => {
    const options = { baseMs: 1_000, maxMs: 1_000, jitter: 0.3 };
    expect(backoffDelay(1, options, () => 0)).toBe(1_000);
    expect(backoffDelay(1, options, () => 1)).toBe(700);
  });
});

describe('retry', () => {
  const fast = { baseMs: 1, maxMs: 1, jitter: 0 };

  it('returns once the operation succeeds and reports each retry', async () => {
    const operation = vi
      .fn<() => Promise<string>>()
      .mockRejectedValueOnce(new Error('first'))
      .mockRejectedValueOnce(new Error('second'))
      .mockResolvedValue('done');
    const onRetry = vi.fn();

    await expect(retry(operation, { ...fast, attempts: 3, onRetry })).resolves.toBe('done');
    expect(operation).toHaveBeenCalledTimes(3);
    expect(onRetry).toHaveBeenCalledTimes(2);
  });

  it('rethrows the last error once the attempts run out', async () => {
    const operation = vi.fn<() => Promise<void>>().mockRejectedValue(new Error('still down'));

    await expect(retry(operation, { ...fast, attempts: 2 })).rejects.toThrow('still down');
    expect(operation).toHaveBeenCalledTimes(2);
  });
});
