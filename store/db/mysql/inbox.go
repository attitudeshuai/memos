package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func (d *DB) CreateInbox(ctx context.Context, create *store.Inbox) (*store.Inbox, error) {
	messageString := "{}"
	if create.Message != nil {
		bytes, err := protojson.Marshal(create.Message)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal inbox message")
		}
		messageString = string(bytes)
	}

	fields := []string{"`sender_id`", "`receiver_id`", "`status`", "`message`"}
	placeholder := []string{"?", "?", "?", "?"}
	args := []any{create.SenderID, create.ReceiverID, create.Status, messageString}

	stmt := "INSERT INTO `inbox` (" + strings.Join(fields, ", ") + ") VALUES (" + strings.Join(placeholder, ", ") + ")"
	result, err := d.db.ExecContext(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	id32 := int32(id)
	inbox, err := d.GetInbox(ctx, &store.FindInbox{ID: &id32})
	if err != nil {
		return nil, err
	}
	return inbox, nil
}

// buildInboxWhere builds the shared filter predicate for inbox queries.
func buildInboxWhere(find *store.FindInbox) ([]string, []any) {
	where, args := []string{"1 = 1"}, []any{}

	if find.ID != nil {
		where, args = append(where, "`id` = ?"), append(args, *find.ID)
	}
	if find.SenderID != nil {
		where, args = append(where, "`sender_id` = ?"), append(args, *find.SenderID)
	}
	if find.ReceiverID != nil {
		where, args = append(where, "`receiver_id` = ?"), append(args, *find.ReceiverID)
	}
	if find.Status != nil {
		where, args = append(where, "`status` = ?"), append(args, *find.Status)
	}
	if find.MessageType != nil {
		// Filter by message type using JSON extraction
		// Note: The type field in JSON is stored as string representation of the enum name
		if *find.MessageType == storepb.InboxMessage_TYPE_UNSPECIFIED {
			where, args = append(where, "(JSON_EXTRACT(`message`, '$.type') IS NULL OR JSON_EXTRACT(`message`, '$.type') = ?)"), append(args, find.MessageType.String())
		} else {
			where, args = append(where, "JSON_EXTRACT(`message`, '$.type') = ?"), append(args, find.MessageType.String())
		}
	}
	if find.CreatedTsBefore != nil {
		where, args = append(where, "UNIX_TIMESTAMP(`created_ts`) < ?"), append(args, *find.CreatedTsBefore)
	}
	if find.MaxID != nil {
		where, args = append(where, "`id` <= ?"), append(args, *find.MaxID)
	}
	return where, args
}

// inboxPlaceholders returns "?, ?, ..." for n values.
func inboxPlaceholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func (d *DB) ListInboxes(ctx context.Context, find *store.FindInbox) ([]*store.Inbox, error) {
	where, args := buildInboxWhere(find)

	query := "SELECT `id`, UNIX_TIMESTAMP(`created_ts`), `sender_id`, `receiver_id`, `status`, `message` FROM `inbox` WHERE " + strings.Join(where, " AND ") + " ORDER BY `created_ts` DESC"
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

	list := []*store.Inbox{}
	for rows.Next() {
		inbox := &store.Inbox{}
		var messageBytes []byte
		if err := rows.Scan(
			&inbox.ID,
			&inbox.CreatedTs,
			&inbox.SenderID,
			&inbox.ReceiverID,
			&inbox.Status,
			&messageBytes,
		); err != nil {
			return nil, err
		}

		message := &storepb.InboxMessage{}
		if err := protojsonUnmarshaler.Unmarshal(messageBytes, message); err != nil {
			return nil, err
		}
		inbox.Message = message
		list = append(list, inbox)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return list, nil
}

func (d *DB) GetInbox(ctx context.Context, find *store.FindInbox) (*store.Inbox, error) {
	list, err := d.ListInboxes(ctx, find)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get inbox")
	}
	if len(list) != 1 {
		return nil, errors.Errorf("unexpected inbox count: %d", len(list))
	}
	return list[0], nil
}

func (d *DB) UpdateInbox(ctx context.Context, update *store.UpdateInbox) (*store.Inbox, error) {
	set, args := []string{"`status` = ?"}, []any{update.Status.String()}
	args = append(args, update.ID)
	query := "UPDATE `inbox` SET " + strings.Join(set, ", ") + " WHERE `id` = ?"
	if _, err := d.db.ExecContext(ctx, query, args...); err != nil {
		return nil, errors.Wrap(err, "failed to update inbox")
	}
	inbox, err := d.GetInbox(ctx, &store.FindInbox{ID: &update.ID})
	if err != nil {
		return nil, err
	}
	return inbox, nil
}

func (d *DB) DeleteInbox(ctx context.Context, delete *store.DeleteInbox) error {
	result, err := d.db.ExecContext(ctx, "DELETE FROM `inbox` WHERE `id` = ?", delete.ID)
	if err != nil {
		return errors.Wrap(err, "failed to delete inbox")
	}
	if _, err := result.RowsAffected(); err != nil {
		return err
	}
	return nil
}

func (d *DB) ListInboxIDs(ctx context.Context, find *store.FindInbox, limit int) ([]int32, error) {
	where, args := buildInboxWhere(find)
	query := "SELECT `id` FROM `inbox` WHERE " + strings.Join(where, " AND ") + " ORDER BY `id` ASC LIMIT ?"
	args = append(args, limit)
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := []int32{}
	for rows.Next() {
		var id int32
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (d *DB) UpdateInboxStatusByIDs(ctx context.Context, ids []int32, status store.InboxStatus) (int32, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	args := make([]any, 0, len(ids)+1)
	args = append(args, status.String())
	for _, id := range ids {
		args = append(args, id)
	}
	query := "UPDATE `inbox` SET `status` = ? WHERE `id` IN (" + inboxPlaceholders(len(ids)) + ")"
	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, errors.Wrap(err, "failed to update inbox status batch")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int32(affected), nil
}

func (d *DB) DeleteInboxByIDs(ctx context.Context, ids []int32) (int32, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	query := "DELETE FROM `inbox` WHERE `id` IN (" + inboxPlaceholders(len(ids)) + ")"
	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, errors.Wrap(err, "failed to delete inbox batch")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int32(affected), nil
}

func (d *DB) GetMaxInboxID(ctx context.Context) (int32, error) {
	var maxID sql.NullInt64
	if err := d.db.QueryRowContext(ctx, "SELECT MAX(`id`) FROM `inbox`").Scan(&maxID); err != nil {
		return 0, err
	}
	if !maxID.Valid {
		return 0, nil
	}
	return int32(maxID.Int64), nil
}

func (d *DB) DeleteInboxesOverPerUserLimit(ctx context.Context, perUserLimit int32, watermark int32) (int32, error) {
	// The derived table is materialized so MySQL does not reject deleting from
	// the table referenced in the subquery (error 1093).
	query := "DELETE FROM `inbox` WHERE `id` IN (" +
		"SELECT `id` FROM (" +
		"SELECT `id`, ROW_NUMBER() OVER (PARTITION BY `receiver_id` ORDER BY `created_ts` DESC, `id` DESC) AS `rn` " +
		"FROM `inbox` WHERE `id` <= ?" +
		") AS ranked WHERE `rn` > ?)"
	result, err := d.db.ExecContext(ctx, query, watermark, perUserLimit)
	if err != nil {
		return 0, errors.Wrap(err, "failed to delete inboxes over per-user limit")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int32(affected), nil
}
