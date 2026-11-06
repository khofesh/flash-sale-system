import { describe, expect, it } from 'vitest';
import { normalizeUserId } from './userId.js';

describe('normalizeUserId', () => {
  it('trims and lowercases a valid id', () => {
    expect(normalizeUserId('  Alice.Example+tag@Example.COM ')).toEqual({
      ok: true,
      value: 'alice.example+tag@example.com',
    });
  });

  it('accepts underscores, digits, dots, plus and hyphens', () => {
    expect(normalizeUserId('user_42.a+b-c')).toEqual({ ok: true, value: 'user_42.a+b-c' });
  });

  it('rejects an empty id', () => {
    expect(normalizeUserId('   ')).toMatchObject({ ok: false });
  });

  it('rejects surrounding-invalid characters', () => {
    expect(normalizeUserId('alice smith')).toMatchObject({ ok: false });
    expect(normalizeUserId('alice/../bob')).toMatchObject({ ok: false });
  });

  it('rejects ids longer than 254 characters', () => {
    expect(normalizeUserId('a'.repeat(254))).toMatchObject({ ok: true });
    expect(normalizeUserId('a'.repeat(255))).toMatchObject({ ok: false });
  });
});
