import { randomUUID } from 'node:crypto';
import os from 'node:os';
import Fastify from 'fastify';
import { HELLO_STREAM, type HelloHop, type HelloStatus } from '@fss/core';
import type { Logger, Pool, Redis } from '@fss/infra';

interface BuildAppDeps {
  redis: Redis;
  pool: Pool;
  logger: Logger;
}

interface HelloRow {
  id: string;
  name: string;
  hops: HelloHop[];
  status: HelloStatus;
  created_at: Date;
  updated_at: Date;
}

const uuidPattern = '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';

export function buildApp({ redis, pool, logger }: BuildAppDeps) {
  const app = Fastify({ loggerInstance: logger });

  app.get('/healthz', async () => ({ status: 'ok' }));

  app.get('/readyz', async (_request, reply) => {
    let redisOk = false;
    let postgresOk = false;

    try {
      await redis.ping();
      redisOk = true;
    } catch (error) {
      logger.warn({ err: error }, 'redis readiness check failed');
    }

    try {
      await pool.query('SELECT 1');
      postgresOk = true;
    } catch (error) {
      logger.warn({ err: error }, 'postgres readiness check failed');
    }

    if (!redisOk || !postgresOk) {
      reply.code(503);
    }

    return {
      redis: redisOk ? 'ok' : 'error',
      postgres: postgresOk ? 'ok' : 'error',
    };
  });

  app.post<{ Body: { name: string } }>(
    '/api/hello',
    {
      schema: {
        body: {
          type: 'object',
          additionalProperties: false,
          required: ['name'],
          properties: {
            name: { type: 'string', minLength: 1, maxLength: 100 },
          },
        },
      },
    },
    async (request, reply) => {
      const name = request.body.name.trim();
      if (!name) {
        return reply.code(400).send({ code: 'VALIDATION_ERROR', message: 'name is required' });
      }

      const id = randomUUID();
      try {
        await redis.xadd(
          HELLO_STREAM,
          '*',
          'id',
          id,
          'name',
          name,
          'hostname',
          os.hostname(),
          'ts',
          new Date().toISOString(),
        );
      } catch (error) {
        logger.error({ err: error }, 'failed to append hello message');
        return reply.code(503).send({ code: 'UNAVAILABLE' });
      }

      return reply.code(202).send({ id });
    },
  );

  app.get<{ Params: { id: string } }>(
    '/api/hello/:id',
    {
      schema: {
        params: {
          type: 'object',
          required: ['id'],
          properties: {
            id: { type: 'string', pattern: uuidPattern },
          },
        },
      },
    },
    async (request, reply) => {
      const { id } = request.params;
      const result = await pool.query<HelloRow>(
        `SELECT id, name, hops, status, created_at, updated_at
           FROM hello_messages
          WHERE id = $1`,
        [id],
      );

      const row = result.rows[0];
      if (!row) {
        // Not persisted yet (briefly normal right after POST) or never arrived.
        return reply.code(404).send({ code: 'NOT_FOUND', id });
      }

      return {
        id: row.id,
        name: row.name,
        status: row.status,
        hops: row.hops,
        createdAt: row.created_at.toISOString(),
        updatedAt: row.updated_at.toISOString(),
      };
    },
  );

  return app;
}
