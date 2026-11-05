import { setTimeout as sleep } from 'node:timers/promises';

const baseUrl = process.env.E2E_BASE_URL ?? 'http://localhost:8080';
const saleId = process.env.SALE_ID ?? 'launch-2026';
const adminToken = process.env.ADMIN_TOKEN ?? 'dev-admin-token';
const timeoutMs = 20_000;

interface SaleView {
  status: 'UPCOMING' | 'ACTIVE' | 'SOLD_OUT' | 'ENDED';
  remainingStock: number;
  totalStock: number;
}

interface PurchaseView {
  userId: string;
  secured: boolean;
  orderId?: string;
  status?: string;
  final?: boolean;
}

async function main(): Promise<void> {
  const startsAt = new Date(Date.now() - 60_000).toISOString();
  const endsAt = new Date(Date.now() + 10 * 60_000).toISOString();

  console.log(`POST ${baseUrl}/admin/sale  sale=${saleId}`);
  const reset = await fetch(`${baseUrl}/admin/sale`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-admin-token': adminToken },
    body: JSON.stringify({ saleId, startsAt, endsAt, totalStock: 100 }),
  });

  if (reset.status === 409) {
    // A previous run left live orders. Reuse the current active sale instead of
    // wiping state; the e2e user id below is unique, so this is still valid.
    console.log('sale already has live orders; continuing with the current sale');
  } else if (!reset.ok) {
    throw new Error(`POST /admin/sale returned ${reset.status}: ${await reset.text()}`);
  }

  const saleResponse = await fetch(`${baseUrl}/api/sale`);
  if (!saleResponse.ok) {
    throw new Error(`GET /api/sale returned ${saleResponse.status}`);
  }
  const sale = (await saleResponse.json()) as SaleView;
  if (sale.status !== 'ACTIVE') {
    throw new Error(
      `Expected an ACTIVE sale, got ${sale.status}. A previous run left orders on a sale that ` +
        'is no longer live; recreate the stack (pnpm stack:down && pnpm stack:up) or run pnpm stress.',
    );
  }
  console.log(`sale is ACTIVE with ${sale.remainingStock}/${sale.totalStock} units`);

  const userId = `e2e-${Date.now()}`;
  const idempotencyKey = `e2e-${userId}`;
  console.log(`POST ${baseUrl}/api/purchases  userId=${userId}`);

  const first = await postPurchase(userId, idempotencyKey);
  if (first.status !== 202) {
    throw new Error(
      `Expected 202 from POST /api/purchases, got ${first.status}: ${await first.text()}`,
    );
  }
  const firstBody = (await first.json()) as { code: string; orderId: string };
  if (firstBody.code !== 'RESERVED' || !firstBody.orderId) {
    throw new Error(`Unexpected purchase body: ${JSON.stringify(firstBody)}`);
  }

  // The same key must replay the original response, not reserve a second unit.
  const replay = await postPurchase(userId, idempotencyKey);
  if (replay.status !== 202) {
    throw new Error(`Expected the idempotent replay to return 202, got ${replay.status}`);
  }
  const replayBody = (await replay.json()) as { code: string; orderId: string };
  if (replayBody.orderId !== firstBody.orderId) {
    throw new Error(
      `Idempotency key returned a different order (${replayBody.orderId} != ${firstBody.orderId})`,
    );
  }

  const deadline = Date.now() + timeoutMs;
  let view: PurchaseView | undefined;
  while (Date.now() < deadline) {
    const response = await fetch(`${baseUrl}/api/purchases/${encodeURIComponent(userId)}`);
    if (response.status === 404) {
      await sleep(200);
      continue;
    }
    if (!response.ok) {
      throw new Error(`GET /api/purchases/${userId} returned ${response.status}`);
    }
    view = (await response.json()) as PurchaseView;
    if (view.final) break;
    await sleep(200);
  }

  if (!view || view.status !== 'COMPLETED') {
    throw new Error(
      view
        ? `Timed out waiting for COMPLETED (last status: ${view.status})`
        : `Timed out: order ${firstBody.orderId} was never persisted`,
    );
  }

  const duplicate = await postPurchase(userId, `e2e-duplicate-${userId}`);
  if (duplicate.status !== 409) {
    throw new Error(`Expected 409 for a second purchase, got ${duplicate.status}`);
  }
  const duplicateBody = (await duplicate.json()) as { code: string; orderId?: string };
  if (duplicateBody.code !== 'ALREADY_PURCHASED') {
    throw new Error(`Expected ALREADY_PURCHASED, got ${duplicateBody.code}`);
  }
  if (duplicateBody.orderId && duplicateBody.orderId !== firstBody.orderId) {
    throw new Error(
      `ALREADY_PURCHASED referenced ${duplicateBody.orderId}, expected ${firstBody.orderId}`,
    );
  }

  const stockDeadline = Date.now() + 5_000;
  let after = (await (await fetch(`${baseUrl}/api/sale`)).json()) as SaleView;
  while (after.remainingStock !== sale.remainingStock - 1 && Date.now() < stockDeadline) {
    // /api/sale is cached for 250 ms per replica, so a read straight after the
    // purchase can still see the old stock.
    await sleep(200);
    after = (await (await fetch(`${baseUrl}/api/sale`)).json()) as SaleView;
  }
  if (after.remainingStock !== sale.remainingStock - 1) {
    throw new Error(
      `Stock did not decrease by exactly one (${sale.remainingStock} -> ${after.remainingStock})`,
    );
  }

  console.log(
    `\nPASS — order ${firstBody.orderId} reached COMPLETED, replay was idempotent, ` +
      `second purchase was 409, stock ${sale.remainingStock} -> ${after.remainingStock}`,
  );
}

async function postPurchase(userId: string, idempotencyKey: string): Promise<Response> {
  return fetch(`${baseUrl}/api/purchases`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'idempotency-key': idempotencyKey,
    },
    body: JSON.stringify({ userId }),
  });
}

main().catch((error: unknown) => {
  console.error('\nFAIL —', error instanceof Error ? error.message : error);
  process.exit(1);
});
