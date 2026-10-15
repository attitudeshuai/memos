package sqlite

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
		DELETE FROM `+"`idempotency_key`"+`
		WHERE `+"`creator_id`"+` = ? AND `+"`idempotency_key`"+` = ?
		  AND (`+"`expires_ts`"+` <= ? OR (`+"`status`"+` = 'IN_FLIGHT' AND `+"`created_ts`"+` <= ?))`,
		insert.CreatorID, insert.Key, nowTs, staleBeforeTs,
	); err != nil {
		return nil, false, err
	}

	result, err := d.db.ExecContext(ctx, `
		INSERT INTO `+"`idempotency_key`"+`
			(`+"`creator_id`"+`, `+"`idempotency_key`"+`, `+"`request_hash`"+`, `+"`status`"+`, `+"`response`"+`, `+"`expires_ts`"+`)
		VALUES (?, ?, ?, 'IN_FLIGHT', ?, ?)
		ON CONFLICT (`+"`creator_id`"+`, `+"`idempotency_key`"+`) DO NOTHING`,
		insert.CreatorID, insert.Key, insert.RequestHash, []byte{}, insert.ExpiresTs,
	)
	if err != nil {
		return nil, false, err
	}
	leader, err := idempotencyRowsAffected(result)
	if err != nil {
		return nil, false, err
	}
	record, err := d.getSQLiteIdempotencyKey(ctx, insert.CreatorID, insert.Key)
	if err != nil {
		return nil, false, err
	}
	return record, leader, nil
}

func (d *DB) GetIdempotencyKey(ctx context.Context, creatorID int32, key string) (*store.IdempotencyRecord, error) {
	return d.getSQLiteIdempotencyKey(ctx, creatorID, key)
}

func (d *DB) getSQLiteIdempotencyKey(ctx context.Context, creatorID int32, key string) (*store.IdempotencyRecord, error) {
	record := &store.IdempotencyRecord{}
	err := d.db.QueryRowContext(ctx, `
		SELECT
			`+"`id`"+`,
			`+"`creator_id`"+`,
			`+"`idempotency_key`"+`,
			`+"`request_hash`"+`,
			`+"`status`"+`,
			`+"`response`"+`,
			`+"`created_ts`"+`,
			`+"`expires_ts`"+`
		FROM `+"`idempotency_key`"+`
		WHERE `+"`creator_id`"+` = ? AND `+"`idempotency_key`"+` = ?
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
		UPDATE `+"`idempotency_key`"+`
		SET `+"`status`"+` = 'COMPLETED', `+"`response`"+` = ?
		WHERE `+"`id`"+` = ? AND `+"`creator_id`"+` = ? AND `+"`idempotency_key`"+` = ? AND `+"`status`"+` = 'IN_FLIGHT'`,
		response, recordID, creatorID, key,
	)
	if err != nil {
		return err
	}
	rows, err := idempotencyRowsAffected(result)
	if err != nil {
		return err
	}
	if !rows {
		return store.ErrIdempotencyKeyNotInFlight
	}
	return nil
}

func (d *DB) ReleaseIdempotencyKey(ctx context.Context, recordID int32, creatorID int32, key string) error {
	_, err := d.db.ExecContext(ctx, `
		DELETE FROM `+"`idempotency_key`"+`
		WHERE `+"`id`"+` = ? AND `+"`creator_id`"+` = ? AND `+"`idempotency_key`"+` = ? AND `+"`status`"+` = 'IN_FLIGHT'`,
		recordID, creatorID, key,
	)
	return err
}

func (d *DB) DeleteExpiredIdempotencyKeys(ctx context.Context, nowTs int64) (int64, error) {
	result, err := d.db.ExecContext(ctx, `
		DELETE FROM `+"`idempotency_key`"+` WHERE `+"`expires_ts`"+` <= ?`,
		nowTs,
	)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	return rows, err
}

// idempotencyRowsAffected reports whether an insert or update touched a row.
func idempotencyRowsAffected(result sql.Result) (bool, error) {
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}
