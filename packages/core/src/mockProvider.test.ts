import { describe, expect, it } from 'vitest';
import { deterministicFailure, mockReference } from './mockProvider.js';

describe('deterministicFailure', () => {
  it('honours the extremes and treats an invalid rate as zero', () => {
    expect(deterministicFailure('order-1', 0, 'payment')).toBe(false);
    expect(deterministicFailure('order-1', -1, 'payment')).toBe(false);
    expect(deterministicFailure('order-1', Number.NaN, 'payment')).toBe(false);
    expect(deterministicFailure('order-1', 1, 'payment')).toBe(true);
    expect(deterministicFailure('order-1', 2, 'payment')).toBe(true);
  });

  it('gives one answer per order and roughly honours the rate', () => {
    const ids = Array.from({ length: 2000 }, (_, index) => `order-${index}`);
    const first = ids.map((id) => deterministicFailure(id, 0.25, 'payment'));
    expect(ids.map((id) => deterministicFailure(id, 0.25, 'payment'))).toEqual(first);

    const rate = first.filter(Boolean).length / ids.length;
    expect(rate).toBeGreaterThan(0.2);
    expect(rate).toBeLessThan(0.3);
  });

  it('keeps payment and shipping outcomes independent', () => {
    const ids = Array.from({ length: 200 }, (_, index) => `order-${index}`);
    expect(ids.map((id) => deterministicFailure(id, 0.5, 'payment'))).not.toEqual(
      ids.map((id) => deterministicFailure(id, 0.5, 'shipping')),
    );
  });
});

describe('mockReference', () => {
  it('derives a stable reference from the order id', () => {
    expect(mockReference('pay', '0123abcd-0000-0000-0000-000000000000')).toBe(
      'pay_0123abcd00000000',
    );
  });
});
