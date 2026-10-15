-- Idempotency records deduplicate create requests: one row per
-- (creator_id, idempotency_key) holds the first request's payload hash and,
-- once finished, its serialized response. Rows expire on expires_ts.
CREATE TABLE IF NOT EXISTS `idempotency_key` (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  creator_id      INTEGER     NOT NULL,
  idempotency_key TEXT        NOT NULL,
  request_hash    TEXT        NOT NULL,
  status          TEXT        NOT NULL CHECK (status IN ('IN_FLIGHT', 'COMPLETED')) DEFAULT 'IN_FLIGHT',
  response        BLOB        NOT NULL DEFAULT (x''),
  created_ts      BIGINT      NOT NULL DEFAULT (strftime('%s', 'now')),
  expires_ts      BIGINT      NOT NULL,
  UNIQUE(`creator_id`, `idempotency_key`)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_key_expires_ts ON idempotency_key(expires_ts);
