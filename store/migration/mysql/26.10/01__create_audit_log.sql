-- audit_log records administrative changes: instance settings, identity
-- providers, storage and usage-limit configuration, and user role or account
-- state changes. Each row is written in the same transaction as the change it
-- describes.
CREATE TABLE `audit_log` (
  `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `created_ts` BIGINT NOT NULL,
  `actor_id` INT NOT NULL,
  `actor_name` VARCHAR(256) NOT NULL DEFAULT '',
  `resource_type` VARCHAR(64) NOT NULL,
  `resource_id` VARCHAR(256) NOT NULL DEFAULT '',
  `action` VARCHAR(32) NOT NULL,
  `changes` LONGTEXT NOT NULL,
  `client_ip` VARCHAR(64) NOT NULL DEFAULT '',
  INDEX `idx_audit_log_resource` (`resource_type`, `resource_id`, `id` DESC),
  INDEX `idx_audit_log_actor` (`actor_id`, `id` DESC),
  INDEX `idx_audit_log_created_ts` (`created_ts`)
);
