package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/usememos/memos/store"
)

// webhookDeliveryColumns is the canonical column order scanned by every
// webhook_delivery read in this package.
var webhookDeliveryColumns = []string{
	"`id`",
	"`created_ts`",
	"`updated_ts`",
	"`user_id`",
	"`webhook_id`",
	"`label`",
	"`url`",
	"`status`",
	"`attempts`",
	"`max_attempts`",
	"`next_due_ts`",
	"`last_attempt_ts`",
	"`settled_ts`",
	"`last_error`",
	"`request_body`",
	"`signing_secret`",
}

func scanWebhookDelivery(row interface {
	Scan(dest ...any) error
}) (*store.WebhookDelivery, error) {
	delivery := &store.WebhookDelivery{}
	if err := row.Scan(
		&delivery.ID,
		&delivery.CreatedTs,
		&delivery.UpdatedTs,
		&delivery.UserID,
		&delivery.WebhookID,
		&delivery.Label,
		&delivery.URL,
		&delivery.Status,
		&delivery.Attempts,
		&delivery.MaxAttempts,
		&delivery.NextDueTs,
		&delivery.LastAttemptTs,
		&delivery.SettledTs,
		&delivery.LastError,
		&delivery.RequestBody,
		&delivery.SigningSecret,
	); err != nil {
		return nil, err
	}
	return delivery, nil
}

func (d *DB) CreateWebhookDelivery(ctx context.Context, create *store.WebhookDelivery) (*store.WebhookDelivery, error) {
	fields := []string{
		"`created_ts`", "`updated_ts`",
		"`user_id`", "`webhook_id`", "`label`", "`url`", "`status`",
		"`attempts`", "`max_attempts`", "`next_due_ts`", "`last_attempt_ts`",
		"`settled_ts`", "`last_error`", "`request_body`", "`signing_secret`",
	}
	placeholders := make([]string, len(fields))
	for i := range fields {
		placeholders[i] = "?"
	}
	args := []any{
		create.CreatedTs, create.UpdatedTs,
		create.UserID, create.WebhookID, create.Label, create.URL, string(create.Status),
		create.Attempts, create.MaxAttempts, create.NextDueTs, create.LastAttemptTs,
		create.SettledTs, create.LastError, create.RequestBody, create.SigningSecret,
	}

	stmt := "INSERT INTO `webhook_delivery` (" + strings.Join(fields, ", ") + ") VALUES (" + strings.Join(placeholders, ", ") + ")"
	result, err := d.db.ExecContext(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	return d.getWebhookDeliveryByID(ctx, id)
}

func (d *DB) getWebhookDeliveryByID(ctx context.Context, id int64) (*store.WebhookDelivery, error) {
	stmt := "SELECT " + strings.Join(webhookDeliveryColumns, ", ") + " FROM `webhook_delivery` WHERE `id` = ?"
	delivery, err := scanWebhookDelivery(d.db.QueryRowContext(ctx, stmt, id))
	if err != nil {
		return nil, err
	}
	return delivery, nil
}

func (d *DB) ListWebhookDeliveries(ctx context.Context, find *store.FindWebhookDeliveries) ([]*store.WebhookDelivery, error) {
	where, args := []string{"1 = 1"}, []any{}

	if find.ID != nil {
		where = append(where, "`id` = ?")
		args = append(args, *find.ID)
	}
	if len(find.IDs) > 0 {
		marks := make([]string, len(find.IDs))
		for i, id := range find.IDs {
			marks[i] = "?"
			args = append(args, id)
		}
		where = append(where, "`id` IN ("+strings.Join(marks, ", ")+")")
	}
	if find.UserID != nil {
		where = append(where, "`user_id` = ?")
		args = append(args, *find.UserID)
	}
	if find.WebhookID != nil {
		where = append(where, "`webhook_id` = ?")
		args = append(args, *find.WebhookID)
	}
	if find.Status != nil {
		where = append(where, "`status` = ?")
		args = append(args, string(*find.Status))
	}

	query := "SELECT " + strings.Join(webhookDeliveryColumns, ", ") + " FROM `webhook_delivery` WHERE " +
		strings.Join(where, " AND ") + " ORDER BY `id` DESC"
	if find.Limit != nil {
		query = fmt.Sprintf("%s LIMIT %d", query, *find.Limit)
		if find.Offset != nil {
			query = fmt.Sprintf("%s OFFSET %d", query, *find.Offset)
		}
	}

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := []*store.WebhookDelivery{}
	for rows.Next() {
		delivery, err := scanWebhookDelivery(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return list, nil
}

func (d *DB) selectDueWebhookDeliveryIDs(ctx context.Context, nowTs int64, limit int) ([]int64, error) {
	selectStmt := "SELECT `id` FROM `webhook_delivery` WHERE `status` IN ('PENDING', 'RETRYING') AND `next_due_ts` <= ? " +
		"ORDER BY `next_due_ts` ASC, `id` ASC LIMIT ?"
	rows, err := d.db.QueryContext(ctx, selectStmt, nowTs, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (d *DB) ClaimDueWebhookDeliveries(ctx context.Context, nowTs int64, limit int) ([]*store.WebhookDelivery, error) {
	ids, err := d.selectDueWebhookDeliveryIDs(ctx, nowTs, limit)
	if err != nil {
		return nil, err
	}

	claimed := make([]*store.WebhookDelivery, 0, len(ids))
	for _, id := range ids {
		updateStmt := "UPDATE `webhook_delivery` SET `status` = 'DELIVERING', `attempts` = `attempts` + 1, " +
			"`last_attempt_ts` = ?, `updated_ts` = ? WHERE `id` = ? " +
			"AND `status` IN ('PENDING', 'RETRYING') AND `next_due_ts` <= ?"
		result, err := d.db.ExecContext(ctx, updateStmt, nowTs, nowTs, id, nowTs)
		if err != nil {
			return nil, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected != 1 {
			// Another worker claimed the row first.
			continue
		}
		delivery, err := d.getWebhookDeliveryByID(ctx, id)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, delivery)
	}
	return claimed, nil
}

func (d *DB) SettleWebhookDelivery(ctx context.Context, settle *store.SettleWebhookDelivery) (bool, error) {
	stmt := "UPDATE `webhook_delivery` SET `status` = ?, `updated_ts` = ?, `next_due_ts` = ?, " +
		"`last_error` = ?, `settled_ts` = ? WHERE `id` = ? AND `status` = 'DELIVERING' AND `attempts` = ?"
	result, err := d.db.ExecContext(ctx, stmt,
		string(settle.Status), settle.UpdatedTs, settle.NextDueTs, settle.LastError, settle.SettledTs,
		settle.ID, settle.Attempts)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected == 1, nil
}

func (d *DB) RecoverInterruptedDeliveries(ctx context.Context, nowTs int64) (int64, error) {
	stmt := "UPDATE `webhook_delivery` SET `status` = 'RETRYING', `next_due_ts` = ?, `updated_ts` = ? " +
		"WHERE `status` = 'DELIVERING'"
	result, err := d.db.ExecContext(ctx, stmt, nowTs, nowTs)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func (d *DB) ReplayWebhookDelivery(ctx context.Context, id int64, nowTs int64) (*store.WebhookDelivery, error) {
	stmt := "UPDATE `webhook_delivery` SET `status` = 'PENDING', `attempts` = 0, `next_due_ts` = ?, " +
		"`last_attempt_ts` = 0, `settled_ts` = 0, `last_error` = '', `updated_ts` = ? " +
		"WHERE `id` = ? AND `status` = 'ABANDONED'"
	result, err := d.db.ExecContext(ctx, stmt, nowTs, nowTs, id)
	if err != nil {
		return nil, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if affected != 1 {
		return nil, nil
	}
	return d.getWebhookDeliveryByID(ctx, id)
}

func (d *DB) prunedWebhookDeliveryIDs(ctx context.Context, userID int32, webhookID string, keep int) ([]any, error) {
	selectStmt := "SELECT `id` FROM `webhook_delivery` WHERE `user_id` = ? AND `webhook_id` = ? " +
		"AND `status` IN ('SUCCEEDED', 'ABANDONED') ORDER BY `id` DESC LIMIT 18446744073709551615 OFFSET ?"
	rows, err := d.db.QueryContext(ctx, selectStmt, userID, webhookID, keep)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []any
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (d *DB) PruneWebhookDeliveries(ctx context.Context, userID int32, webhookID string, keep int) error {
	ids, err := d.prunedWebhookDeliveryIDs(ctx, userID, webhookID, keep)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	marks := make([]string, len(ids))
	for i := range ids {
		marks[i] = "?"
	}
	_, err = d.db.ExecContext(ctx, "DELETE FROM `webhook_delivery` WHERE `id` IN ("+strings.Join(marks, ", ")+")", ids...)
	return err
}
