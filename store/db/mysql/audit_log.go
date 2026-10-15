package mysql

import (
	"context"
	"database/sql"
	"strings"

	"github.com/pkg/errors"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

// insertAuditLogTx writes one audit entry within tx.
func insertAuditLogTx(ctx context.Context, tx *sql.Tx, create *store.AuditLog) error {
	changes := create.Changes
	if changes == "" {
		changes = "[]"
	}
	stmt := "INSERT INTO `audit_log` (`created_ts`, `actor_id`, `actor_name`, `resource_type`, `resource_id`, `action`, `changes`, `client_ip`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	if _, err := tx.ExecContext(ctx, stmt,
		create.CreatedTs,
		create.ActorID,
		create.ActorName,
		create.ResourceType.String(),
		create.ResourceID,
		create.Action.String(),
		changes,
		create.ClientIP,
	); err != nil {
		return errors.Wrap(err, "failed to insert audit log")
	}
	return nil
}

// pruneAuditLogsTx enforces retention within tx. The nested derived table is
// required by MySQL, which cannot DELETE from a table while selecting it
// directly in the WHERE clause.
func pruneAuditLogsTx(ctx context.Context, tx *sql.Tx, maxRecords int64, beforeSec int64) error {
	if beforeSec > 0 {
		if _, err := tx.ExecContext(ctx, "DELETE FROM `audit_log` WHERE `created_ts` < ?", beforeSec); err != nil {
			return errors.Wrap(err, "failed to prune audit logs by age")
		}
	}
	if maxRecords > 0 {
		stmt := "DELETE FROM `audit_log` WHERE `id` NOT IN (" +
			"SELECT `id` FROM (SELECT `id` FROM `audit_log` ORDER BY `id` DESC LIMIT ?) AS audit_log_keep)"
		if _, err := tx.ExecContext(ctx, stmt, maxRecords); err != nil {
			return errors.Wrap(err, "failed to prune audit logs by count")
		}
	}
	return nil
}

func (d *DB) CreateAuditLog(ctx context.Context, create *store.AuditLog, maxRecords int64, beforeSec int64) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin audit log transaction")
	}
	defer func() { _ = tx.Rollback() }()
	if err := insertAuditLogTx(ctx, tx, create); err != nil {
		return err
	}
	if err := pruneAuditLogsTx(ctx, tx, maxRecords, beforeSec); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) PruneAuditLogs(ctx context.Context, maxRecords int64, beforeSec int64) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin audit log prune transaction")
	}
	defer func() { _ = tx.Rollback() }()
	if err := pruneAuditLogsTx(ctx, tx, maxRecords, beforeSec); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) ListAuditLogs(ctx context.Context, find *store.FindAuditLog) ([]*store.AuditLog, error) {
	where, args := []string{"1 = 1"}, []any{}
	if v := find.ResourceType; v != nil && *v != storepb.AuditLog_RESOURCE_TYPE_UNSPECIFIED {
		where, args = append(where, "`resource_type` = ?"), append(args, v.String())
	}
	if find.ResourceID != "" {
		where, args = append(where, "`resource_id` = ?"), append(args, find.ResourceID)
	}
	if v := find.ActorID; v != nil {
		where, args = append(where, "`actor_id` = ?"), append(args, *v)
	}
	if v := find.Action; v != nil && *v != storepb.AuditLog_ACTION_UNSPECIFIED {
		where, args = append(where, "`action` = ?"), append(args, v.String())
	}

	stmt := "SELECT `id`, `created_ts`, `actor_id`, `actor_name`, `resource_type`, `resource_id`, `action`, `changes`, `client_ip` FROM `audit_log` WHERE " +
		strings.Join(where, " AND ") + " ORDER BY `id` DESC"
	if v := find.Limit; v != nil {
		stmt += " LIMIT ?"
		args = append(args, *v)
		if v := find.Offset; v != nil && *v > 0 {
			stmt += " OFFSET ?"
			args = append(args, *v)
		}
	}

	rows, err := d.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list audit logs")
	}
	defer rows.Close()

	list := []*store.AuditLog{}
	for rows.Next() {
		auditLog := &store.AuditLog{}
		var resourceType, action string
		if err := rows.Scan(
			&auditLog.ID,
			&auditLog.CreatedTs,
			&auditLog.ActorID,
			&auditLog.ActorName,
			&resourceType,
			&auditLog.ResourceID,
			&action,
			&auditLog.Changes,
			&auditLog.ClientIP,
		); err != nil {
			return nil, errors.Wrap(err, "failed to scan audit log")
		}
		auditLog.ResourceType = storepb.AuditLog_ResourceType(storepb.AuditLog_ResourceType_value[resourceType])
		auditLog.Action = storepb.AuditLog_Action(storepb.AuditLog_Action_value[action])
		list = append(list, auditLog)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to iterate audit logs")
	}
	return list, nil
}

func (d *DB) UpsertInstanceSettingAudited(ctx context.Context, upsert *store.InstanceSetting, audit *store.AuditLog, maxRecords int64, beforeSec int64) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin instance setting audit transaction")
	}
	defer func() { _ = tx.Rollback() }()
	if err := upsertInstanceSetting(ctx, tx, upsert); err != nil {
		return err
	}
	if err := insertAuditLogTx(ctx, tx, audit); err != nil {
		return err
	}
	if err := pruneAuditLogsTx(ctx, tx, maxRecords, beforeSec); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) CreateIdentityProviderAudited(ctx context.Context, create *store.IdentityProvider, audit *store.AuditLog, maxRecords int64, beforeSec int64) (*store.IdentityProvider, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin identity provider audit transaction")
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx, "INSERT INTO `idp` (`uid`, `name`, `type`, `identifier_filter`, `config`) VALUES (?, ?, ?, ?, ?)",
		create.UID, create.Name, create.Type.String(), create.IdentifierFilter, create.Config)
	if err != nil {
		return nil, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	create.ID = int32(id)
	if err := insertAuditLogTx(ctx, tx, audit); err != nil {
		return nil, err
	}
	if err := pruneAuditLogsTx(ctx, tx, maxRecords, beforeSec); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return create, nil
}

func (d *DB) UpdateIdentityProviderAudited(ctx context.Context, update *store.UpdateIdentityProvider, audit *store.AuditLog, maxRecords int64, beforeSec int64) (*store.IdentityProvider, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin identity provider audit transaction")
	}
	defer func() { _ = tx.Rollback() }()

	set, args := []string{}, []any{}
	if v := update.Name; v != nil {
		set, args = append(set, "`name` = ?"), append(args, *v)
	}
	if v := update.IdentifierFilter; v != nil {
		set, args = append(set, "`identifier_filter` = ?"), append(args, *v)
	}
	if v := update.Config; v != nil {
		set, args = append(set, "`config` = ?"), append(args, *v)
	}
	args = append(args, update.ID)
	if _, err := tx.ExecContext(ctx, "UPDATE `idp` SET "+strings.Join(set, ", ")+" WHERE `id` = ?", args...); err != nil {
		return nil, err
	}

	var identityProvider store.IdentityProvider
	var typeString string
	if err := tx.QueryRowContext(ctx, "SELECT `id`, `uid`, `name`, `type`, `identifier_filter`, `config` FROM `idp` WHERE `id` = ?", update.ID).Scan(
		&identityProvider.ID,
		&identityProvider.UID,
		&identityProvider.Name,
		&typeString,
		&identityProvider.IdentifierFilter,
		&identityProvider.Config,
	); err != nil {
		return nil, err
	}
	identityProvider.Type = storepb.IdentityProvider_Type(storepb.IdentityProvider_Type_value[typeString])
	if err := insertAuditLogTx(ctx, tx, audit); err != nil {
		return nil, err
	}
	if err := pruneAuditLogsTx(ctx, tx, maxRecords, beforeSec); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &identityProvider, nil
}

func (d *DB) WriteUsageLimitSettingAudited(ctx context.Context, name string, exists bool, oldValue, newValue string, audit *store.AuditLog, maxRecords int64, beforeSec int64) (bool, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return false, errors.Wrap(err, "failed to begin usage limit audit transaction")
	}
	defer func() { _ = tx.Rollback() }()

	var written bool
	if !exists {
		result, execErr := tx.ExecContext(ctx, "INSERT IGNORE INTO `system_setting` (`name`, `value`, `description`) VALUES (?, ?, '')", name, newValue)
		if execErr != nil {
			return false, errors.Wrap(execErr, "failed to conditionally create usage limits")
		}
		affected, execErr := result.RowsAffected()
		if execErr != nil {
			return false, errors.Wrap(execErr, "failed to inspect usage limit insert")
		}
		written = affected == 1
	} else {
		// MySQL reports zero affected rows when the value is unchanged. Read the
		// row under the same transaction instead of treating that as a conflict.
		if oldValue == newValue {
			var count int
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM `system_setting` WHERE `name` = ? AND `value` = CAST(? AS BINARY)", name, oldValue).Scan(&count); err != nil {
				return false, errors.Wrap(err, "failed to read usage limits")
			}
			written = count == 1
		} else {
			result, execErr := tx.ExecContext(ctx, "UPDATE `system_setting` SET `value` = ? WHERE `name` = ? AND `value` = CAST(? AS BINARY)", newValue, name, oldValue)
			if execErr != nil {
				return false, errors.Wrap(execErr, "failed to conditionally update usage limits")
			}
			affected, execErr := result.RowsAffected()
			if execErr != nil {
				return false, errors.Wrap(execErr, "failed to inspect usage limit update")
			}
			written = affected == 1
		}
	}
	if !written {
		return false, nil
	}
	if err := insertAuditLogTx(ctx, tx, audit); err != nil {
		return false, err
	}
	if err := pruneAuditLogsTx(ctx, tx, maxRecords, beforeSec); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (d *DB) UpdateUserAudited(ctx context.Context, update *store.UpdateUser, audit *store.AuditLog, maxRecords int64, beforeSec int64) (*store.User, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	user, err := updateUserTx(ctx, tx, update)
	if err != nil {
		return nil, err
	}
	if err := insertAuditLogTx(ctx, tx, audit); err != nil {
		return nil, err
	}
	if err := pruneAuditLogsTx(ctx, tx, maxRecords, beforeSec); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return user, nil
}
