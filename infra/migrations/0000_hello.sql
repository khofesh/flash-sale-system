CREATE TABLE IF NOT EXISTS hello_messages (
  id         UUID PRIMARY KEY,
  name       TEXT NOT NULL,
  hops       JSONB NOT NULL DEFAULT '[]'::jsonb,
  status     TEXT NOT NULL DEFAULT 'QUEUED',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
