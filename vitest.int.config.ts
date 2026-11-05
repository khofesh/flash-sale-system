import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

const fromRoot = (path: string) => fileURLToPath(new URL(path, import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      '@fss/config': fromRoot('./packages/config/src/index.ts'),
      '@fss/core': fromRoot('./packages/core/src/index.ts'),
      '@fss/infra': fromRoot('./packages/infra/src/index.ts'),
    },
  },
  test: {
    include: ['tests/**/*.int.test.ts'],
    environment: 'node',
    testTimeout: 120_000,
    hookTimeout: 180_000,
    fileParallelism: false,
  },
});
