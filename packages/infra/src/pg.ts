import { Pool } from 'pg';

export type { Pool };

export function createPgPool(connectionString: string): Pool {
  return new Pool({
    connectionString,
    max: 10,
    idleTimeoutMillis: 30_000,
  });
}
