import { setTimeout as sleep } from 'node:timers/promises';
const baseUrl = process.env.E2E_BASE_URL ?? 'http://localhost:8080';
const expectedHops = ['api', 'orchestrator', 'payment', 'orchestrator', 'shipping', 'orchestrator'];
const timeoutMs = 15_000;

interface Hop {
  service: string;
  hostname: string;
  at: string;
}

interface HelloView {
  id: string;
  name?: string;
  status: 'QUEUED' | 'PAID' | 'DONE';
  hops: Hop[];
}

async function main(): Promise<void> {
  const name = `e2e-${Date.now()}`;
  console.log(`POST ${baseUrl}/api/hello  name=${name}`);

  const post = await fetch(`${baseUrl}/api/hello`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ name }),
  });

  if (post.status !== 202) {
    throw new Error(`Expected 202 from POST /api/hello, got ${post.status}: ${await post.text()}`);
  }

  const { id } = (await post.json()) as { id: string };
  console.log(`queued as ${id}; polling for DONE…`);

  const deadline = Date.now() + timeoutMs;
  let view: HelloView | undefined;

  while (Date.now() < deadline) {
    const response = await fetch(`${baseUrl}/api/hello/${id}`);
    if (response.status === 404) {
      // Not persisted by the orchestrator yet.
      await sleep(200);
      continue;
    }
    if (!response.ok) {
      throw new Error(`GET /api/hello/${id} returned ${response.status}`);
    }
    view = (await response.json()) as HelloView;
    if (view.status === 'DONE') break;
    await sleep(200);
  }

  if (!view || view.status !== 'DONE') {
    throw new Error(
      view
        ? `Timed out after ${timeoutMs} ms waiting for DONE (last status: ${view.status})`
        : `Timed out after ${timeoutMs} ms: hello ${id} was never persisted (404 throughout)`,
    );
  }

  const services = view.hops.map((hop) => hop.service);
  console.log('\nHop timeline:');
  for (const [index, hop] of view.hops.entries()) {
    console.log(
      `  ${String(index + 1).padStart(2)}. ${hop.service.padEnd(12)} ${hop.hostname.padEnd(20)} ${hop.at}`,
    );
  }

  if (JSON.stringify(services) !== JSON.stringify(expectedHops)) {
    throw new Error(
      `Unexpected hops.\n  expected: ${expectedHops.join(' → ')}\n  actual:   ${services.join(' → ')}`,
    );
  }

  console.log(`\nPASS — ${view.hops.length} hops, final status ${view.status}`);
}

main().catch((error: unknown) => {
  console.error('\nFAIL —', error instanceof Error ? error.message : error);
  process.exit(1);
});
