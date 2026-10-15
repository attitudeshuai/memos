package mysql

import (
	"context"
	"database/sql"
	stderrors "errors"

	"github.com/pkg/errors"

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
		return nil, false, errors.Wrap(err, "failed to purge stale idempotency key")
	}

	result, err := d.db.ExecContext(ctx, `
		INSERT IGNORE INTO `+"`idempotency_key`"+`
			(`+"`creator_id`"+`, `+"`idempotency_key`"+`, `+"`request_hash`"+`, `+"`status`"+`, `+"`response`"+`, `+"`expires_ts`"+`)
		VALUES (?, ?, ?, 'IN_FLIGHT', ?, ?)`,
		insert.CreatorID, insert.Key, insert.RequestHash, []byte{}, insert.ExpiresTs,
	)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to acquire idempotency key")
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to inspect idempotency key insert")
	}
	leader := rowsAffected == 1
	record, err := d.getMySQLIdempotencyKey(ctx, insert.CreatorID, insert.Key)
	if err != nil {
		return nil, false, err
	}
	return record, leader, nil
}

func (d *DB) GetIdempotencyKey(ctx context.Context, creatorID int32, key string) (*store.IdempotencyRecord, error) {
	return d.getMySQLIdempotencyKey(ctx, creatorID, key)
}

func (d *DB) getMySQLIdempotencyKey(ctx context.Context, creatorID int32, key string) (*store.IdempotencyRecord, error) {
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
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "failed to get idempotency key")
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
		return errors.Wrap(err, "failed to complete idempotency key")
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to inspect idempotency key completion")
	}
	if rowsAffected != 1 {
		return store.ErrIdempotencyKeyNotInFlight
	}
	return nil
}

func (d *DB) ReleaseIdempotencyKey(ctx context.Context, recordID int32, creatorID int32, key string) error {
	if _, err := d.db.ExecContext(ctx, `
		DELETE FROM `+"`idempotency_key`"+`
		WHERE `+"`id`"+` = ? AND `+"`creator_id`"+` = ? AND `+"`idempotency_key`"+` = ? AND `+"`status`"+` = 'IN_FLIGHT'`,
		recordID, creatorID, key,
	); err != nil {
		return errors.Wrap(err, "failed to release idempotency key")
	}
	return nil
}

func (d *DB) DeleteExpiredIdempotencyKeys(ctx context.Context, nowTs int64) (int64, error) {
	result, err := d.db.ExecContext(ctx, `
		DELETE FROM `+"`idempotency_key`"+` WHERE `+"`expires_ts`"+` <= ?`,
		nowTs,
	)
	if err != nil {
		return 0, errors.Wrap(err, "failed to delete expired idempotency keys")
	}
	rows, err := result.RowsAffected()
	return rows, err
}
