import { setTimeout as sleep } from 'node:timers/promises';

export interface BackoffOptions {
  /** Delay before the first retry, in milliseconds. */
  baseMs?: number;
  /** Upper bound for a single delay, in milliseconds. */
  maxMs?: number;
  /**
   * Fraction of the delay that is randomised (0–1). The minimum wait is
   * `1 - jitter` of the exponential delay, so replicas do not retry in lockstep.
   */
  jitter?: number;
}

export interface RetryOptions extends BackoffOptions {
  /** Total attempts, including the first one. */
  attempts?: number;
  onRetry?: (error: unknown, attempt: number, delayMs: number) => void;
}

/** Exponential backoff with equal jitter. `attempt` is 1-based. */
export function backoffDelay(
  attempt: number,
  options: BackoffOptions = {},
  random: () => number = Math.random,
): number {
  const baseMs = options.baseMs ?? 100;
  const maxMs = options.maxMs ?? 5_000;
  const jitter = Math.min(1, Math.max(0, options.jitter ?? 0.2));
  const exponential = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.max(0, Math.round(exponential * (1 - jitter * random())));
}

/**
 * Runs an operation, retrying failures with exponential backoff and jitter.
 * The last error is rethrown once the attempts are exhausted, so callers can
 * leave the work for redelivery (stream reclaim or SQS visibility timeout).
 */
export async function retry<T>(
  operation: () => Promise<T>,
  options: RetryOptions = {},
): Promise<T> {
  const attempts = Math.max(1, options.attempts ?? 5);

  for (let attempt = 1; ; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      if (attempt >= attempts) throw error;

      const delayMs = backoffDelay(attempt, options);
      options.onRetry?.(error, attempt, delayMs);
      await sleep(delayMs);
    }
  }
}
