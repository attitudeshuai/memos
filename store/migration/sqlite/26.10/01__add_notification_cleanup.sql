CREATE INDEX IF NOT EXISTS idx_inbox_receiver_created ON inbox(receiver_id, created_ts, id);

CREATE TABLE notification_cleanup_run (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_ts BIGINT NOT NULL DEFAULT (strftime('%s', 'now')),
  complete_ts BIGINT NOT NULL DEFAULT 0,
  trigger TEXT NOT NULL,
  status TEXT NOT NULL,
  watermark_inbox_id INTEGER NOT NULL DEFAULT 0,
  max_age_days INTEGER NOT NULL DEFAULT 0,
  max_per_user INTEGER NOT NULL DEFAULT 0,
  age_cutoff_ts BIGINT NOT NULL DEFAULT 0,
  deleted_age_count INTEGER NOT NULL DEFAULT 0,
  deleted_cap_count INTEGER NOT NULL DEFAULT 0,
  error_detail TEXT NOT NULL DEFAULT ''
);
