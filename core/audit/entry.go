// Package audit builds administrative change entries for the audit ledger.
// It owns the rules for which fields a ledger entry records and how sensitive
// values are masked; the store layer only persists the entries it produces.
package audit

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

// FieldChange is the value of one key field before and after a change.
type FieldChange struct {
	Field  string `json:"field"`
	Before string `json:"before"`
	After  string `json:"after"`
}

// Change builds one field change.
func Change(field, before, after string) FieldChange {
	return FieldChange{Field: field, Before: before, After: after}
}

// Entry is an administrative change before it is persisted.
type Entry struct {
	ActorID       int32
	ActorUsername string

	ResourceType storepb.AuditLog_ResourceType
	// ResourceID identifies the object within its type: an instance setting
	// key, an identity provider UID, a usage limit UID, or a username.
	ResourceID string
	Action     storepb.AuditLog_Action

	Changes  []FieldChange
	ClientIP string
}

// NewEntry builds an entry for an acting administrator.
func NewEntry(actorID int32, actorUsername string, resourceType storepb.AuditLog_ResourceType, action storepb.AuditLog_Action, resourceID string, changes []FieldChange, clientIP string) *Entry {
	return &Entry{
		ActorID:       actorID,
		ActorUsername: actorUsername,
		ResourceType:  resourceType,
		ResourceID:    resourceID,
		Action:        action,
		Changes:       changes,
		ClientIP:      clientIP,
	}
}

// ToStore converts the entry to its persistable form, serializing the field
// changes. Sensitive values must already be masked in Changes.
func (e *Entry) ToStore() (*store.AuditLog, error) {
	changes := e.Changes
	if changes == nil {
		changes = []FieldChange{}
	}
	value, err := json.Marshal(changes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal audit changes")
	}
	return &store.AuditLog{
		ActorID:      e.ActorID,
		ActorName:    e.ActorUsername,
		ResourceType: e.ResourceType,
		ResourceID:   e.ResourceID,
		Action:       e.Action,
		Changes:      string(value),
		ClientIP:     e.ClientIP,
	}, nil
}

// DecodeChanges parses the stored change list of a ledger row.
func DecodeChanges(raw string) []FieldChange {
	if strings.TrimSpace(raw) == "" {
		return []FieldChange{}
	}
	changes := []FieldChange{}
	if err := json.Unmarshal([]byte(raw), &changes); err != nil {
		return []FieldChange{}
	}
	return changes
}
