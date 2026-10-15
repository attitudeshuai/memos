CREATE INDEX `idx_inbox_receiver_created` ON `inbox`(`receiver_id`, `created_ts`, `id`);

CREATE TABLE `notification_cleanup_run` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `started_ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `complete_ts` BIGINT NOT NULL DEFAULT 0,
  `trigger` TEXT NOT NULL,
  `status` TEXT NOT NULL,
  `watermark_inbox_id` INT NOT NULL DEFAULT 0,
  `max_age_days` INT NOT NULL DEFAULT 0,
  `max_per_user` INT NOT NULL DEFAULT 0,
  `age_cutoff_ts` BIGINT NOT NULL DEFAULT 0,
  `deleted_age_count` INT NOT NULL DEFAULT 0,
  `deleted_cap_count` INT NOT NULL DEFAULT 0,
  `error_detail` TEXT NOT NULL
);
