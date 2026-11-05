CREATE TABLE IF NOT EXISTS sales (
  id          TEXT PRIMARY KEY,
  product_id  TEXT        NOT NULL,
  starts_at   TIMESTAMPTZ NOT NULL,
  ends_at     TIMESTAMPTZ NOT NULL,
  total_stock INT         NOT NULL CHECK (total_stock >= 0),
  CHECK (ends_at > starts_at)
);
