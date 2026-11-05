import type { Logger } from './logger.js';

export function onShutdown(logger: Logger, cleanup: () => Promise<void>, timeoutMs = 10_000): void {
  let shuttingDown = false;

  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutting down');

    const timeout = setTimeout(() => {
      logger.error('graceful shutdown timed out');
      process.exit(1);
    }, timeoutMs);

    try {
      await cleanup();
    } catch (error) {
      logger.error({ err: error }, 'shutdown failed');
      process.exitCode = 1;
    } finally {
      clearTimeout(timeout);
      process.exit();
    }
  };

  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}
