import { Redis } from 'ioredis';

export type { Redis };

export function createRedis(url: string): Redis {
  return new Redis(url, {
    maxRetriesPerRequest: 2,
    enableReadyCheck: true,
    lazyConnect: false,
  });
}
