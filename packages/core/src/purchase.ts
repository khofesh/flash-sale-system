/** Outcomes returned by `reserve.lua` (SPEC §5.2). */
export type ReserveOutcome =
  | 'RESERVED'
  | 'ALREADY_PURCHASED'
  | 'SOLD_OUT'
  | 'NOT_STARTED'
  | 'ENDED'
  | 'NOT_FOUND'
  | 'UNAVAILABLE';

export type ReserveResult =
  | { status: 'RESERVED'; reservationId: string }
  | { status: 'ALREADY_PURCHASED' }
  | { status: 'SOLD_OUT' }
  | { status: 'NOT_STARTED' }
  | { status: 'ENDED' }
  | { status: 'NOT_FOUND' }
  | { status: 'UNAVAILABLE' };

export type OrderStatus =
  | 'RESERVED'
  | 'PAYMENT_PENDING'
  | 'PAID'
  | 'SHIPPING_PENDING'
  | 'COMPLETED'
  | 'SHIPPING_FAILED'
  | 'CANCELLED'
  | 'REFUNDED';

/** Statuses that mean the user currently holds (or has completed) a unit. */
export const SECURED_ORDER_STATUSES: ReadonlySet<OrderStatus> = new Set<OrderStatus>([
  'RESERVED',
  'PAYMENT_PENDING',
  'PAID',
  'SHIPPING_PENDING',
  'COMPLETED',
]);

export const FINAL_ORDER_STATUSES: ReadonlySet<OrderStatus> = new Set<OrderStatus>([
  'COMPLETED',
  'SHIPPING_FAILED',
  'CANCELLED',
  'REFUNDED',
]);

export type PurchaseCode =
  | 'RESERVED'
  | 'ALREADY_PURCHASED'
  | 'SOLD_OUT'
  | 'NOT_STARTED'
  | 'ENDED'
  | 'VALIDATION_ERROR'
  | 'RATE_LIMITED'
  | 'UNAVAILABLE'
  | 'SALE_NOT_FOUND';

export interface PurchaseResponse {
  statusCode: number;
  body: { code: PurchaseCode; orderId?: string };
  /** Set when the client should retry, e.g. Redis is temporarily unavailable. */
  retryAfter?: number;
}

/** Maps a Lua reserve result to the HTTP response required by SPEC §6.2. */
export function purchaseResponse(result: ReserveResult, orderId?: string): PurchaseResponse {
  switch (result.status) {
    case 'RESERVED':
      return {
        statusCode: 202,
        body: { code: 'RESERVED', orderId: orderId ?? result.reservationId },
      };
    case 'ALREADY_PURCHASED':
      return {
        statusCode: 409,
        body: { code: 'ALREADY_PURCHASED', ...(orderId ? { orderId } : {}) },
      };
    case 'SOLD_OUT':
      return { statusCode: 410, body: { code: 'SOLD_OUT' } };
    case 'NOT_STARTED':
      return { statusCode: 403, body: { code: 'NOT_STARTED' } };
    case 'ENDED':
      return { statusCode: 403, body: { code: 'ENDED' } };
    case 'NOT_FOUND':
      return { statusCode: 404, body: { code: 'SALE_NOT_FOUND' } };
    case 'UNAVAILABLE':
      return { statusCode: 503, body: { code: 'UNAVAILABLE' }, retryAfter: 1 };
  }
}

/** Parses the array reply of `reserve.lua`. Unknown replies are a hard error. */
export function parseReserveReply(reply: unknown): ReserveResult {
  if (!Array.isArray(reply) || typeof reply[0] !== 'string') {
    throw new Error(`Malformed reserve reply: ${JSON.stringify(reply)}`);
  }

  const outcome = reply[0] as ReserveOutcome;
  if (outcome === 'RESERVED') {
    if (typeof reply[1] !== 'string' || reply[1].length === 0) {
      throw new Error(`Reserved reply has no reservationId: ${JSON.stringify(reply)}`);
    }
    return { status: 'RESERVED', reservationId: reply[1] };
  }

  switch (outcome) {
    case 'ALREADY_PURCHASED':
    case 'SOLD_OUT':
    case 'NOT_STARTED':
    case 'ENDED':
    case 'NOT_FOUND':
    case 'UNAVAILABLE':
      return { status: outcome };
    default:
      throw new Error(`Unknown reserve outcome: ${String(outcome)}`);
  }
}
