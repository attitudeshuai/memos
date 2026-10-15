-- audit_log records administrative changes: instance settings, identity
-- providers, storage and usage-limit configuration, and user role or account
-- state changes. Each row is written in the same transaction as the change it
-- describes.
CREATE TABLE audit_log (
  id BIGSERIAL PRIMARY KEY,
  created_ts BIGINT NOT NULL,
  actor_id INTEGER NOT NULL,
  actor_name TEXT NOT NULL DEFAULT '',
  resource_type TEXT NOT NULL,
  resource_id TEXT NOT NULL DEFAULT '',
  action TEXT NOT NULL,
  changes TEXT NOT NULL DEFAULT '[]',
  client_ip TEXT NOT NULL DEFAULT ''
);

CREATE INDEX idx_audit_log_resource ON audit_log(resource_type, resource_id, id DESC);
CREATE INDEX idx_audit_log_actor ON audit_log(actor_id, id DESC);
CREATE INDEX idx_audit_log_created_ts ON audit_log(created_ts);
