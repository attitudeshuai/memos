package store

// NotificationCleanupTrigger records what started a notification cleanup run.
type NotificationCleanupTrigger string

const (
	// CleanupTriggerScheduled marks a run started by the instance scheduler.
	CleanupTriggerScheduled NotificationCleanupTrigger = "SCHEDULED"
	// CleanupTriggerManual marks a run started by an administrator.
	CleanupTriggerManual NotificationCleanupTrigger = "MANUAL"
)

// NotificationCleanupStatus is the outcome state of a cleanup run.
type NotificationCleanupStatus string

const (
	// CleanupStatusRunning means the run has started and not finished.
	CleanupStatusRunning NotificationCleanupStatus = "RUNNING"
	// CleanupStatusSucceeded means the run completed without an error.
	CleanupStatusSucceeded NotificationCleanupStatus = "SUCCEEDED"
	// CleanupStatusFailed means the run aborted; ErrorDetail carries the reason.
	CleanupStatusFailed NotificationCleanupStatus = "FAILED"
)

// NotificationCleanupRun is the audit record of one notification retention
// cleanup. Timestamps are unix seconds, mirroring the inbox schema.
type NotificationCleanupRun struct {
	ID         int32
	StartedTs  int64
	CompleteTs int64 // 0 while the run is still in progress.

	Trigger NotificationCleanupTrigger
	Status  NotificationCleanupStatus

	// WatermarkInboxID is the snapshot of MAX(inbox.id) taken before any
	// deletion; only IDs up to it were eligible.
	WatermarkInboxID int32

	// Policy snapshot for this run. Zero disables the corresponding rule.
	MaxAgeDays  int32
	MaxPerUser  int32
	AgeCutoffTs int64 // 0 when the age rule was disabled.

	DeletedAgeCount int32
	DeletedCapCount int32

	// ErrorDetail holds the failure reason for FAILED runs.
	ErrorDetail string
}
