import { access, readdir, readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { join, resolve } from 'node:path';
import type { Pool } from 'pg';

export async function runMigrations(pool: Pool, migrationsDir: string): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      filename TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  const files = (await readdir(migrationsDir)).filter((file) => file.endsWith('.sql')).sort();

  for (const file of files) {
    const applied = await pool.query('SELECT 1 FROM schema_migrations WHERE filename = $1', [file]);
    if (applied.rowCount) continue;

    const sql = await readFile(join(migrationsDir, file), 'utf8');
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(sql);
      await client.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file]);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}

export async function resolveMigrationsDir(explicit?: string): Promise<string> {
  const candidates = [
    explicit,
    process.env.MIGRATIONS_DIR,
    resolve(process.cwd(), 'infra/migrations'),
    fileURLToPath(new URL('../../../infra/migrations', import.meta.url)),
  ].filter((candidate): candidate is string => Boolean(candidate));

  for (const candidate of candidates) {
    try {
      await access(candidate);
      return candidate;
    } catch {
      // try the next candidate
    }
  }

  throw new Error(`Migrations directory not found. Tried: ${candidates.join(', ')}`);
}
