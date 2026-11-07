import { describe, expect, it } from 'vitest';
import { FINAL_ORDER_STATUSES, parseReserveReply, purchaseResponse } from './purchase.ts';

describe('purchaseResponse', () => {
  const cases = [
    [{ status: 'RESERVED', reservationId: 'id-1' } as const, 202, 'RESERVED'],
    [{ status: 'ALREADY_PURCHASED' } as const, 409, 'ALREADY_PURCHASED'],
    [{ status: 'SOLD_OUT' } as const, 410, 'SOLD_OUT'],
    [{ status: 'NOT_STARTED' } as const, 403, 'NOT_STARTED'],
    [{ status: 'ENDED' } as const, 403, 'ENDED'],
    [{ status: 'NOT_FOUND' } as const, 404, 'SALE_NOT_FOUND'],
    [{ status: 'UNAVAILABLE' } as const, 503, 'UNAVAILABLE'],
  ] as const;

  for (const [result, statusCode, code] of cases) {
    it(`maps ${result.status} to ${statusCode} ${code}`, () => {
      const response = purchaseResponse(result);
      expect(response.statusCode).toBe(statusCode);
      expect(response.body.code).toBe(code);
    });
  }

  it('returns the reservation id as the order id', () => {
    expect(purchaseResponse({ status: 'RESERVED', reservationId: 'res-1' }).body.orderId).toBe(
      'res-1',
    );
  });

  it('marks an unavailable response as retryable', () => {
    expect(purchaseResponse({ status: 'UNAVAILABLE' }).retryAfter).toBe(1);
  });
});

describe('FINAL_ORDER_STATUSES', () => {
  it('excludes SHIPPING_FAILED, which still moves on to REFUNDED', () => {
    expect(FINAL_ORDER_STATUSES.has('SHIPPING_FAILED')).toBe(false);
    expect(FINAL_ORDER_STATUSES.has('REFUNDED')).toBe(true);
  });
});

describe('parseReserveReply', () => {
  it('parses a reservation', () => {
    expect(parseReserveReply(['RESERVED', 'res-1'])).toEqual({
      status: 'RESERVED',
      reservationId: 'res-1',
    });
  });

  it('parses terminal outcomes', () => {
    expect(parseReserveReply(['SOLD_OUT'])).toEqual({ status: 'SOLD_OUT' });
    expect(parseReserveReply(['ALREADY_PURCHASED'])).toEqual({ status: 'ALREADY_PURCHASED' });
  });

  it('rejects malformed replies', () => {
    expect(() => parseReserveReply('nope')).toThrow();
    expect(() => parseReserveReply(['RESERVED'])).toThrow();
    expect(() => parseReserveReply(['WHATEVER'])).toThrow();
  });
});
