import os from 'node:os';
import pino, { type Logger, type LoggerOptions } from 'pino';

export type { Logger };

export function createLogger(service: string, level: LoggerOptions['level'] = 'info'): Logger {
  return pino({
    level,
    base: {
      service,
      hostname: os.hostname(),
    },
    timestamp: pino.stdTimeFunctions.isoTime,
  });
}
