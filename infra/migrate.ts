import { setTimeout as sleep } from 'node:timers/promises';
import { loadConfig } from '@fss/config';
import { createLogger, createPgPool, resolveMigrationsDir, runMigrations } from '@fss/infra';

const config = loadConfig();
const logger = createLogger('migrate', config.LOG_LEVEL);
const pool = createPgPool(config.DATABASE_URL);
const migrationsDir = await resolveMigrationsDir();

const maxAttempts = 30;

for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
  try {
    await runMigrations(pool, migrationsDir);
    logger.info({ migrationsDir }, 'migrations applied');
    await pool.end();
    process.exit(0);
  } catch (error) {
    logger.warn({ err: error, attempt, maxAttempts }, 'migration failed; retrying');
    await sleep(1_000);
  }
}

logger.error('migrations failed after all attempts');
process.exit(1);
