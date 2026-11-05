import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const scenario = process.argv.slice(2).find((argument) => argument !== '--') ?? 'smoke';
const available = ['smoke'];

if (!available.includes(scenario)) {
  console.error(`Unknown stress scenario "${scenario}". Available: ${available.join(', ')}`);
  process.exit(1);
}

const baseUrl = process.env.BASE_URL ?? 'http://host.docker.internal:8080';
const scriptPath = resolve(process.cwd(), 'stress', `${scenario}.js`);

const result = spawnSync(
  'docker',
  [
    'run',
    '--rm',
    '-i',
    '--add-host',
    'host.docker.internal:host-gateway',
    '-e',
    `BASE_URL=${baseUrl}`,
    'grafana/k6:0.57.0',
    'run',
    '-',
  ],
  // The script goes in on stdin: no bind mount, so no SELinux relabeling under Podman.
  { input: readFileSync(scriptPath), stdio: ['pipe', 'inherit', 'inherit'] },
);

process.exit(result.status ?? 1);
