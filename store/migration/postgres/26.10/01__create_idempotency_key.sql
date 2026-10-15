-- Idempotency records deduplicate create requests: one row per
-- (creator_id, idempotency_key) holds the first request's payload hash and,
-- once finished, its serialized response. Rows expire on expires_ts.
CREATE TABLE IF NOT EXISTS idempotency_key (
  id              SERIAL       PRIMARY KEY,
  creator_id      INTEGER      NOT NULL,
  idempotency_key VARCHAR(255) NOT NULL,
  request_hash    VARCHAR(64)  NOT NULL,
  status          VARCHAR(16)  NOT NULL DEFAULT 'IN_FLIGHT' CHECK (status IN ('IN_FLIGHT', 'COMPLETED')),
  response        BYTEA        NOT NULL DEFAULT ''::BYTEA,
  created_ts      BIGINT       NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()),
  expires_ts      BIGINT       NOT NULL,
  UNIQUE (creator_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_key_expires_ts ON idempotency_key(expires_ts);
