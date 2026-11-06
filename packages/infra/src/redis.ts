import { Redis } from 'ioredis';

export type { Redis };

export interface RedisOptions {
  /**
   * For request paths: a command fails at once while disconnected and after
   * `commandTimeout` when Redis stalls, so callers can answer 503 quickly. It is
   * never resent after a reconnect either, because a reserve whose outcome was
   * already reported as an error must not run later. Unsuitable for blocking
   * commands such as XREADGROUP, and commands sent before `redisReady` fail.
   */
  failFast?: boolean;
  /**
   * Batches concurrent commands from one connection into a single write. Only
   * for connections that never issue blocking commands, e.g. the request path.
   */
  autoPipelining?: boolean;
}

const FAIL_FAST_COMMAND_TIMEOUT_MS = 1_000;

export function createRedis(
  url: string,
  { failFast = false, autoPipelining = false }: RedisOptions = {},
): Redis {
  return new Redis(url, {
    maxRetriesPerRequest: 2,
    enableReadyCheck: true,
    lazyConnect: false,
    enableAutoPipelining: autoPipelining,
    // A server that vanishes sends no FIN. Reconnects instead of waiting on the dead
    // socket; this must stay longer than any XREADGROUP BLOCK.
    socketTimeout: 10_000,
    keepAlive: 1_000,
    ...(failFast
      ? {
          enableOfflineQueue: false,
          autoResendUnfulfilledCommands: false,
          commandTimeout: FAIL_FAST_COMMAND_TIMEOUT_MS,
          retryStrategy: (times: number) => Math.min(times * 50, 500),
        }
      : {}),
  });
}

/** Resolves once the client can take commands; rejects after `timeoutMs`. */
export async function redisReady(redis: Redis, timeoutMs = 10_000): Promise<void> {
  if (redis.status === 'ready') return;

  await new Promise<void>((resolve, reject) => {
    const onReady = () => {
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      redis.off('ready', onReady);
      reject(new Error(`Redis was not ready within ${timeoutMs} ms`));
    }, timeoutMs);
    redis.once('ready', onReady);
  });
}
