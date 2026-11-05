import { describe, expect, it } from 'vitest';
import { hello, makeHop } from './index.js';

describe('hello', () => {
  it('greets the given name', () => {
    expect(hello('World')).toBe('Hello, World!');
  });

  it('records a hop with an ISO timestamp', () => {
    const at = new Date('2026-01-02T03:04:05.000Z');
    expect(makeHop('api', 'api-1', at)).toEqual({
      service: 'api',
      hostname: 'api-1',
      at: '2026-01-02T03:04:05.000Z',
    });
  });
});
