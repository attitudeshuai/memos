package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/usememos/memos/store"
)

func (d *DB) AcquireIdempotencyKey(ctx context.Context, insert *store.IdempotencyKeyInsert, nowTs, staleBeforeTs int64) (*store.IdempotencyRecord, bool, error) {
	// Drop a record that has either outlived its retention or stayed IN_FLIGHT
	// past the takeover timeout. The unique index makes the subsequent insert
	// the single leader-election point, even under concurrent callers.
	if _, err := d.db.ExecContext(ctx, `
		DELETE FROM idempotency_key
		WHERE creator_id = $1 AND idempotency_key = $2
		  AND (expires_ts <= $3 OR (status = 'IN_FLIGHT' AND created_ts <= $4))`,
		insert.CreatorID, insert.Key, nowTs, staleBeforeTs,
	); err != nil {
		return nil, false, err
	}

	result, err := d.db.ExecContext(ctx, `
		INSERT INTO idempotency_key
			(creator_id, idempotency_key, request_hash, status, response, expires_ts)
		VALUES ($1, $2, $3, 'IN_FLIGHT', $4, $5)
		ON CONFLICT (creator_id, idempotency_key) DO NOTHING`,
		insert.CreatorID, insert.Key, insert.RequestHash, []byte{}, insert.ExpiresTs,
	)
	if err != nil {
		return nil, false, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	leader := rowsAffected == 1
	record, err := d.getPostgresIdempotencyKey(ctx, insert.CreatorID, insert.Key)
	if err != nil {
		return nil, false, err
	}
	return record, leader, nil
}

func (d *DB) GetIdempotencyKey(ctx context.Context, creatorID int32, key string) (*store.IdempotencyRecord, error) {
	return d.getPostgresIdempotencyKey(ctx, creatorID, key)
}

func (d *DB) getPostgresIdempotencyKey(ctx context.Context, creatorID int32, key string) (*store.IdempotencyRecord, error) {
	record := &store.IdempotencyRecord{}
	err := d.db.QueryRowContext(ctx, `
		SELECT
			id,
			creator_id,
			idempotency_key,
			request_hash,
			status,
			response,
			created_ts,
			expires_ts
		FROM idempotency_key
		WHERE creator_id = $1 AND idempotency_key = $2
		LIMIT 1`,
		creatorID, key,
	).Scan(
		&record.ID,
		&record.CreatorID,
		&record.Key,
		&record.RequestHash,
		&record.Status,
		&record.Response,
		&record.CreatedTs,
		&record.ExpiresTs,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return record, nil
}

func (d *DB) CompleteIdempotencyKey(ctx context.Context, recordID int32, creatorID int32, key string, response []byte) error {
	result, err := d.db.ExecContext(ctx, `
		UPDATE idempotency_key
		SET status = 'COMPLETED', response = $1
		WHERE id = $2 AND creator_id = $3 AND idempotency_key = $4 AND status = 'IN_FLIGHT'`,
		response, recordID, creatorID, key,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return store.ErrIdempotencyKeyNotInFlight
	}
	return nil
}

func (d *DB) ReleaseIdempotencyKey(ctx context.Context, recordID int32, creatorID int32, key string) error {
	_, err := d.db.ExecContext(ctx, `
		DELETE FROM idempotency_key
		WHERE id = $1 AND creator_id = $2 AND idempotency_key = $3 AND status = 'IN_FLIGHT'`,
		recordID, creatorID, key,
	)
	return err
}

func (d *DB) DeleteExpiredIdempotencyKeys(ctx context.Context, nowTs int64) (int64, error) {
	result, err := d.db.ExecContext(ctx, `
		DELETE FROM idempotency_key WHERE expires_ts <= $1`,
		nowTs,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
