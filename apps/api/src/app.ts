import { randomUUID } from 'node:crypto';
import os from 'node:os';
import Fastify from 'fastify';
import {
  HELLO_STREAM,
  deriveSaleStatus,
  type HelloHop,
  type HelloStatus,
  type SaleStatus,
} from '@fss/core';
import {
  getSaleSnapshot,
  resetSale,
  type Logger,
  type Pool,
  type Redis,
  type SaleSnapshot,
} from '@fss/infra';

interface BuildAppDeps {
  redis: Redis;
  pool: Pool;
  logger: Logger;
  saleId?: string;
  productId?: string;
  adminToken?: string;
  saleCacheMs?: number;
}

interface HelloRow {
  id: string;
  name: string;
  hops: HelloHop[];
  status: HelloStatus;
  created_at: Date;
  updated_at: Date;
}

interface SaleView {
  saleId: string;
  status: SaleStatus;
  startsAt: string;
  endsAt: string;
  remainingStock: number;
  totalStock: number;
  serverTime: string;
}

interface AdminSaleBody {
  saleId?: string;
  productId?: string;
  startsAt: string;
  endsAt: string;
  totalStock: number;
}

const uuidPattern = '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';

export function buildApp({
  redis,
  pool,
  logger,
  saleId = 'launch-2026',
  productId = 'flash-tshirt',
  adminToken = 'dev-admin-token',
  saleCacheMs = 250,
}: BuildAppDeps) {
  const app = Fastify({ loggerInstance: logger });

  let saleCache: { at: number; sale: SaleSnapshot | null } | null = null;

  const recentSale = async (now: number): Promise<SaleSnapshot | null> => {
    if (saleCache && now - saleCache.at < saleCacheMs) {
      return saleCache.sale;
    }
    const sale = await getSaleSnapshot(redis, saleId);
    saleCache = { at: Date.now(), sale };
    return sale;
  };

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

  app.get('/api/sale', async (_request, reply) => {
    const now = Date.now();
    let sale: SaleSnapshot | null;

    try {
      sale = await recentSale(now);
    } catch (error) {
      logger.error({ err: error }, 'failed to read sale from redis');
      return reply.code(503).send({ code: 'UNAVAILABLE' });
    }

    if (!sale) {
      return reply.code(404).send({ code: 'SALE_NOT_FOUND' });
    }

    const view: SaleView = {
      saleId: sale.id,
      status: deriveSaleStatus(
        { startsAt: sale.startsAt, endsAt: sale.endsAt },
        sale.remainingStock,
        now,
      ),
      startsAt: new Date(sale.startsAt).toISOString(),
      endsAt: new Date(sale.endsAt).toISOString(),
      remainingStock: sale.remainingStock,
      totalStock: sale.totalStock,
      serverTime: new Date(now).toISOString(),
    };

    return view;
  });

  app.post<{ Body: AdminSaleBody }>(
    '/admin/sale',
    {
      onRequest: async (request, reply) => {
        if (request.headers['x-admin-token'] !== adminToken) {
          return reply.code(401).send({ code: 'UNAUTHORIZED' });
        }
      },
      schema: {
        body: {
          type: 'object',
          additionalProperties: false,
          required: ['startsAt', 'endsAt', 'totalStock'],
          properties: {
            saleId: { type: 'string', minLength: 1, maxLength: 100 },
            productId: { type: 'string', minLength: 1, maxLength: 100 },
            startsAt: { type: 'string', format: 'date-time' },
            endsAt: { type: 'string', format: 'date-time' },
            totalStock: { type: 'integer', minimum: 0 },
          },
        },
      },
    },
    async (request, reply) => {
      const { startsAt, endsAt, totalStock } = request.body;
      const start = new Date(startsAt);
      const end = new Date(endsAt);
      if (
        Number.isNaN(start.getTime()) ||
        Number.isNaN(end.getTime()) ||
        end.getTime() <= start.getTime()
      ) {
        return reply
          .code(400)
          .send({ code: 'VALIDATION_ERROR', message: 'endsAt must be after startsAt' });
      }

      const targetSaleId = request.body.saleId ?? saleId;
      try {
        await resetSale(pool, redis, {
          id: targetSaleId,
          productId: request.body.productId ?? productId,
          startsAt: start,
          endsAt: end,
          totalStock,
        });
      } catch (error) {
        logger.error({ err: error, saleId: targetSaleId }, 'failed to reset sale');
        return reply.code(503).send({ code: 'UNAVAILABLE' });
      }

      if (targetSaleId === saleId) {
        saleCache = null;
      }

      logger.info({ saleId: targetSaleId, startsAt, endsAt, totalStock }, 'sale created or reset');

      return reply.code(200).send({
        saleId: targetSaleId,
        startsAt: start.toISOString(),
        endsAt: end.toISOString(),
        totalStock,
        remainingStock: totalStock,
      });
    },
  );

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
