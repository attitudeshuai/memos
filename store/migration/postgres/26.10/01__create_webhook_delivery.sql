-- webhook_delivery persists the lifecycle ledger of outbound webhook deliveries.
CREATE TABLE webhook_delivery (
  id BIGSERIAL PRIMARY KEY,
  created_ts BIGINT NOT NULL,
  updated_ts BIGINT NOT NULL,
  user_id INTEGER NOT NULL,
  webhook_id VARCHAR(64) NOT NULL,
  label TEXT NOT NULL DEFAULT '',
  url TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'DELIVERING', 'SUCCEEDED', 'RETRYING', 'ABANDONED')),
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL,
  next_due_ts BIGINT NOT NULL DEFAULT 0,
  last_attempt_ts BIGINT NOT NULL DEFAULT 0,
  settled_ts BIGINT NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '',
  request_body TEXT NOT NULL,
  signing_secret TEXT NOT NULL DEFAULT ''
);

CREATE INDEX idx_webhook_delivery_hook ON webhook_delivery(user_id, webhook_id, id);
CREATE INDEX idx_webhook_delivery_due ON webhook_delivery(status, next_due_ts);
