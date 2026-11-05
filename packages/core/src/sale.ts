export type SaleStatus = 'UPCOMING' | 'ACTIVE' | 'SOLD_OUT' | 'ENDED';

/** The time window of a sale, in epoch milliseconds. */
export interface SaleWindow {
  startsAt: number;
  endsAt: number;
}

/**
 * Derives the sale status from its window and the remaining stock (SPEC §4.1).
 * Status is never stored: every caller derives it from the same rule.
 */
export function deriveSaleStatus(meta: SaleWindow, stock: number, now: number): SaleStatus {
  if (now < meta.startsAt) return 'UPCOMING';
  if (now >= meta.endsAt) return 'ENDED';
  if (stock <= 0) return 'SOLD_OUT';
  return 'ACTIVE';
}
