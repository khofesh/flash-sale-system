import { Pool } from 'pg';
import type { Logger } from './logger.js';

export type { Pool, PoolClient } from 'pg';

export function createPgPool(connectionString: string, logger?: Logger): Pool {
  const pool = new Pool({
    connectionString,
    max: 10,
    idleTimeoutMillis: 30_000,
    // A server that vanishes (killed, partitioned) sends no FIN, so a query on its
    // socket would otherwise wait for minutes, hanging whichever loop issued it.
    connectionTimeoutMillis: 5_000,
    query_timeout: 5_000,
    keepAlive: true,
  });

  const onError = (error: Error) => {
    if (logger) logger.warn({ err: error }, 'postgres connection failed');
    else console.warn(`postgres connection failed: ${error.message}`);
  };

  // A client losing its connection (Postgres restarted) emits 'error', and with no
  // listener that is an uncaught exception. The pool only listens while a client is
  // idle and re-emits it here; a checked-out client, say mid-transaction, needs its
  // own. The query in flight rejects either way, and the caller discards the client.
  pool.on('error', onError);
  pool.on('connect', (client) => client.on('error', onError));

  return pool;
}
