import { readFileSync } from 'node:fs';
import { parseReserveReply, type ReserveResult } from '@fss/core';
import type { Logger } from './logger.ts';
import type { Redis } from './redis.ts';
import { saleRedisKeys } from './saleRepo.ts';

const reserveSource = readFileSync(new URL('./redis/reserve.lua', import.meta.url), 'utf8');
const releaseSource = readFileSync(new URL('./redis/release.lua', import.meta.url), 'utf8');

export interface ReservationStore {
  reserve(saleId: string, userId: string, reservationId: string): Promise<ReserveResult>;
  /**
   * Returns the unit held by this reservation. Returns false when it is not the
   * user's current holder: already released, or a stale order after a re-purchase.
   * Throws `SaleStockMissingError` while the sale awaits a rebuild, so the release
   * stays pending and is retried.
   */
  release(saleId: string, userId: string, reservationId: string): Promise<boolean>;
  /** The reservation currently holding the user's unit, if any. */
  holder(saleId: string, userId: string): Promise<string | null>;
  isBuyer(saleId: string, userId: string): Promise<boolean>;
}

/** The sale has no Redis stock key: it is failed closed until a rebuild runs. */
export class SaleStockMissingError extends Error {
  constructor(saleId: string) {
    super(`Sale ${saleId} has no Redis stock; it is awaiting a rebuild`);
    this.name = 'SaleStockMissingError';
  }
}

interface ScriptState {
  reserve?: string;
  release?: string;
}

/**
 * Runs the reservation scripts by SHA (SPEC §5.2). Scripts are loaded once at
 * startup; a `NOSCRIPT` reply (Redis restarted or its script cache was flushed)
 * loads them again and retries, so a restart cannot take purchases down.
 */
export class RedisReservationStore implements ReservationStore {
  private readonly shas: ScriptState = {};

  constructor(
    private readonly redis: Redis,
    private readonly logger?: Logger,
  ) {}

  async loadScripts(): Promise<void> {
    this.shas.reserve = await this.load('reserve', reserveSource);
    this.shas.release = await this.load('release', releaseSource);
  }

  async reserve(saleId: string, userId: string, reservationId: string): Promise<ReserveResult> {
    const keys = saleRedisKeys(saleId);
    const reply = await this.evaluate(
      'reserve',
      reserveSource,
      5,
      [keys.meta, keys.stock, keys.buyers, keys.reservations, keys.holders],
      [userId, reservationId],
    );
    return parseReserveReply(reply);
  }

  async release(saleId: string, userId: string, reservationId: string): Promise<boolean> {
    const keys = saleRedisKeys(saleId);
    const reply = await this.evaluate(
      'release',
      releaseSource,
      3,
      [keys.buyers, keys.stock, keys.holders],
      [userId, reservationId],
    );
    if (Number(reply) === -1) throw new SaleStockMissingError(saleId);
    return Number(reply) === 1;
  }

  async holder(saleId: string, userId: string): Promise<string | null> {
    return this.redis.hget(saleRedisKeys(saleId).holders, userId);
  }

  async isBuyer(saleId: string, userId: string): Promise<boolean> {
    return (await this.redis.sismember(saleRedisKeys(saleId).buyers, userId)) === 1;
  }

  private async evaluate(
    name: 'reserve' | 'release',
    source: string,
    numKeys: number,
    keys: string[],
    args: string[],
  ): Promise<unknown> {
    let sha = this.shas[name];
    if (!sha) {
      sha = await this.load(name, source);
    }

    try {
      return await this.redis.evalsha(sha, numKeys, ...keys, ...args);
    } catch (error) {
      if (!isNoScript(error)) throw error;
      this.logger?.warn({ script: name }, 'redis script cache was lost; reloading');
      sha = await this.load(name, source);
      return this.redis.evalsha(sha, numKeys, ...keys, ...args);
    }
  }

  private async load(name: 'reserve' | 'release', source: string): Promise<string> {
    const sha = await this.redis.script('LOAD', source);
    this.shas[name] = String(sha);
    return this.shas[name];
  }
}

function isNoScript(error: unknown): boolean {
  return error instanceof Error && error.message.includes('NOSCRIPT');
}
