import { describe, expect, it } from 'vitest';
import { deriveSaleStatus } from './sale.js';

const startsAt = Date.parse('2026-01-01T10:00:00.000Z');
const endsAt = Date.parse('2026-01-01T11:00:00.000Z');
const window = { startsAt, endsAt };

describe('deriveSaleStatus', () => {
  it('is UPCOMING one millisecond before the start', () => {
    expect(deriveSaleStatus(window, 100, startsAt - 1)).toBe('UPCOMING');
  });

  it('is ACTIVE exactly at the start when stock remains', () => {
    expect(deriveSaleStatus(window, 100, startsAt)).toBe('ACTIVE');
  });

  it('is ACTIVE one millisecond before the end when stock remains', () => {
    expect(deriveSaleStatus(window, 1, endsAt - 1)).toBe('ACTIVE');
  });

  it('is ENDED exactly at the end', () => {
    expect(deriveSaleStatus(window, 100, endsAt)).toBe('ENDED');
  });

  it('is SOLD_OUT inside the window with no stock', () => {
    expect(deriveSaleStatus(window, 0, startsAt)).toBe('SOLD_OUT');
  });

  it('is ENDED even when no stock remains', () => {
    expect(deriveSaleStatus(window, 0, endsAt)).toBe('ENDED');
  });
});
