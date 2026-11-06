CREATE TABLE IF NOT EXISTS orders (
  id             UUID PRIMARY KEY,
  sale_id        TEXT        NOT NULL REFERENCES sales(id),
  user_id        TEXT        NOT NULL,
  status         TEXT        NOT NULL,
  payment_ref    TEXT,
  shipment_ref   TEXT,
  failure_reason TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- At most one live order per user per sale. Cancelled and refunded orders do
-- not count, so a user can try again after compensation released their unit.
CREATE UNIQUE INDEX IF NOT EXISTS orders_one_live_per_user
  ON orders (sale_id, user_id)
  WHERE status NOT IN ('CANCELLED', 'REFUNDED');

CREATE INDEX IF NOT EXISTS orders_sale_user_idx ON orders (sale_id, user_id);

CREATE TABLE IF NOT EXISTS saga_events (
  id         BIGSERIAL PRIMARY KEY,
  order_id   UUID        NOT NULL REFERENCES orders(id),
  type       TEXT        NOT NULL,
  payload    JSONB       NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS saga_events_order_idx ON saga_events (order_id);

CREATE TABLE IF NOT EXISTS processed_messages (
  consumer   TEXT NOT NULL,
  message_id TEXT NOT NULL,
  PRIMARY KEY (consumer, message_id)
);
