-- Set once compensation has returned the order's unit to Redis. A dead order
-- (CANCELLED/REFUNDED) with this column NULL still owes a release, which the
-- orchestrator's sweeper retries until it succeeds.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS released_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS orders_release_pending_idx
  ON orders (updated_at)
  WHERE status IN ('CANCELLED', 'REFUNDED') AND released_at IS NULL;
