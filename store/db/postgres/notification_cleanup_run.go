package postgres

import (
	"context"

	"github.com/usememos/memos/store"
)

func (d *DB) CreateNotificationCleanupRun(ctx context.Context, run *store.NotificationCleanupRun) (*store.NotificationCleanupRun, error) {
	stmt := `
INSERT INTO notification_cleanup_run
  (started_ts, complete_ts, trigger, status, watermark_inbox_id, max_age_days, max_per_user, age_cutoff_ts, deleted_age_count, deleted_cap_count, error_detail)
VALUES
  ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING id`
	args := []any{
		run.StartedTs, run.CompleteTs, string(run.Trigger), string(run.Status), run.WatermarkInboxID,
		run.MaxAgeDays, run.MaxPerUser, run.AgeCutoffTs, run.DeletedAgeCount,
		run.DeletedCapCount, run.ErrorDetail,
	}
	if err := d.db.QueryRowContext(ctx, stmt, args...).Scan(&run.ID); err != nil {
		return nil, err
	}
	return run, nil
}

func (d *DB) UpdateNotificationCleanupRun(ctx context.Context, run *store.NotificationCleanupRun) error {
	stmt := `
UPDATE notification_cleanup_run
SET complete_ts = $1, status = $2, age_cutoff_ts = $3, deleted_age_count = $4,
    deleted_cap_count = $5, error_detail = $6
WHERE id = $7`
	result, err := d.db.ExecContext(ctx, stmt,
		run.CompleteTs, string(run.Status), run.AgeCutoffTs, run.DeletedAgeCount,
		run.DeletedCapCount, run.ErrorDetail, run.ID,
	)
	if err != nil {
		return err
	}
	if _, err := result.RowsAffected(); err != nil {
		return err
	}
	return nil
}

func (d *DB) ListNotificationCleanupRuns(ctx context.Context, limit int) ([]*store.NotificationCleanupRun, error) {
	query := `
SELECT id, started_ts, complete_ts, trigger, status, watermark_inbox_id,
       max_age_days, max_per_user, age_cutoff_ts, deleted_age_count,
       deleted_cap_count, error_detail
FROM notification_cleanup_run
ORDER BY started_ts DESC, id DESC
LIMIT $1`
	rows, err := d.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNotificationCleanupRuns(rows)
}

// cleanupRunRows is the subset of *sql.Rows used while scanning run records.
type cleanupRunRows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

func scanNotificationCleanupRuns(rows cleanupRunRows) ([]*store.NotificationCleanupRun, error) {
	defer rows.Close()
	list := []*store.NotificationCleanupRun{}
	for rows.Next() {
		run := &store.NotificationCleanupRun{}
		var trigger, status string
		if err := rows.Scan(
			&run.ID,
			&run.StartedTs,
			&run.CompleteTs,
			&trigger,
			&status,
			&run.WatermarkInboxID,
			&run.MaxAgeDays,
			&run.MaxPerUser,
			&run.AgeCutoffTs,
			&run.DeletedAgeCount,
			&run.DeletedCapCount,
			&run.ErrorDetail,
		); err != nil {
			return nil, err
		}
		run.Trigger = store.NotificationCleanupTrigger(trigger)
		run.Status = store.NotificationCleanupStatus(status)
		list = append(list, run)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return list, nil
}

func (d *DB) PruneNotificationCleanupRuns(ctx context.Context, keep int) error {
	query := `
DELETE FROM notification_cleanup_run
WHERE id NOT IN (
  SELECT id FROM (
    SELECT id FROM notification_cleanup_run ORDER BY id DESC LIMIT $1
  ) AS kept
)`
	result, err := d.db.ExecContext(ctx, query, keep)
	if err != nil {
		return err
	}
	if _, err := result.RowsAffected(); err != nil {
		return err
	}
	return nil
}
