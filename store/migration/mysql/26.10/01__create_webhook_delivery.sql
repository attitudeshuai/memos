-- webhook_delivery persists the lifecycle ledger of outbound webhook deliveries.
CREATE TABLE `webhook_delivery` (
  `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `created_ts` BIGINT NOT NULL,
  `updated_ts` BIGINT NOT NULL,
  `user_id` INT NOT NULL,
  `webhook_id` VARCHAR(64) NOT NULL,
  `label` TEXT NOT NULL,
  `url` TEXT NOT NULL,
  `status` VARCHAR(32) NOT NULL,
  `attempts` INT NOT NULL DEFAULT 0,
  `max_attempts` INT NOT NULL,
  `next_due_ts` BIGINT NOT NULL DEFAULT 0,
  `last_attempt_ts` BIGINT NOT NULL DEFAULT 0,
  `settled_ts` BIGINT NOT NULL DEFAULT 0,
  `last_error` TEXT NOT NULL,
  `request_body` MEDIUMTEXT NOT NULL,
  `signing_secret` TEXT NOT NULL,
  CONSTRAINT `chk_webhook_delivery_status` CHECK (`status` IN ('PENDING', 'DELIVERING', 'SUCCEEDED', 'RETRYING', 'ABANDONED'))
);

CREATE INDEX `idx_webhook_delivery_hook` ON `webhook_delivery`(`user_id`, `webhook_id`, `id`);
CREATE INDEX `idx_webhook_delivery_due` ON `webhook_delivery`(`status`, `next_due_ts`);
