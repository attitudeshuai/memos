package store

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	storepb "github.com/usememos/memos/proto/gen/store"
)

// AuditLog is one administrative change ledger entry. Rows are append-only:
// every audited business mutation writes its entry in the same transaction as
// the mutation, so a committed change always has its entry and vice versa.
type AuditLog struct {
	ID        int64
	CreatedTs int64

	ActorID   int32
	ActorName string

	ResourceType storepb.AuditLog_ResourceType
	ResourceID   string
	Action       storepb.AuditLog_Action

	// Changes is the raw JSON array of {"field","before","after"} entries.
	// Sensitive values are masked before the entry reaches the store.
	Changes  string
	ClientIP string
}

// FindAuditLog filters ListAuditLogs. A nil or zero-valued field matches every
// entry.
type FindAuditLog struct {
	ResourceType *storepb.AuditLog_ResourceType
	ResourceID   string
	ActorID      *int32
	Action       *storepb.AuditLog_Action

	Limit  *int
	Offset *int
}

// auditRetentionSettingName is the system_setting row holding the declared
// retention policy as raw JSON. It is not part of the InstanceSettingKey enum:
// like the audit table itself, it is managed through the audit API only.
const auditRetentionSettingName = "audit.retention"

const (
	// DefaultAuditMaxRecords is the retained-entry bound when none is declared.
	DefaultAuditMaxRecords int64 = 10000
	// DefaultAuditRetainDays is the retained-age bound when none is declared.
	DefaultAuditRetainDays int32 = 365
	// MaxAuditMaxRecords bounds a declared count so a mistaken value cannot
	// disable pruning effectively forever.
	MaxAuditMaxRecords int64 = 1000000
	// MaxAuditRetainDays bounds a declared age to roughly one century.
	MaxAuditRetainDays int32 = 36500
)

// AuditRetention declares how much ledger history is kept.
// MaxRecords 0 disables the count bound; RetainDays 0 disables the age bound.
type AuditRetention struct {
	MaxRecords int64 `json:"maxRecords"`
	RetainDays int32 `json:"retainDays"`
}

// Validate checks the bounds of a declared retention policy.
func (r AuditRetention) Validate() error {
	if r.MaxRecords < 0 || r.MaxRecords > MaxAuditMaxRecords {
		return errors.Errorf("audit maxRecords must be between 0 and %d", MaxAuditMaxRecords)
	}
	if r.RetainDays < 0 || r.RetainDays > MaxAuditRetainDays {
		return errors.Errorf("audit retainDays must be between 0 and %d", MaxAuditRetainDays)
	}
	return nil
}

// Bounds turns the declaration into pruning parameters against now:
// maxRecords keeps the newest entries of that count; beforeSec is an inclusive
// unix-second cutoff for age pruning, or 0 when the age bound is disabled.
func (r AuditRetention) Bounds(now time.Time) (maxRecords int64, beforeSec int64) {
	if r.RetainDays > 0 {
		beforeSec = now.AddDate(0, 0, -int(r.RetainDays)).Unix()
	}
	return r.MaxRecords, beforeSec
}

// auditRetentionCache caches the declared retention without sharing the
// instance-setting cache, whose entries are typed instance settings.
//
// The audit write path reads this on every administrative mutation, so a miss
// must not add a write or a network round trip beyond the single row read.
type auditRetentionCache struct {
	mu      sync.RWMutex
	loaded  bool
	current AuditRetention
}

func (s *Store) auditRetentionCached() (AuditRetention, bool) {
	s.auditRetention.mu.RLock()
	defer s.auditRetention.mu.RUnlock()
	if !s.auditRetention.loaded {
		return AuditRetention{}, false
	}
	return s.auditRetention.current, true
}

func (s *Store) setAuditRetentionCached(r AuditRetention) {
	s.auditRetention.mu.Lock()
	defer s.auditRetention.mu.Unlock()
	s.auditRetention.loaded = true
	s.auditRetention.current = r
}

// GetAuditRetention returns the declared retention policy, or the defaults.
func (s *Store) GetAuditRetention(ctx context.Context) (AuditRetention, error) {
	if cached, ok := s.auditRetentionCached(); ok {
		return cached, nil
	}
	rows, err := s.driver.ListInstanceSettings(ctx, &FindInstanceSetting{Name: auditRetentionSettingName})
	if err != nil {
		return AuditRetention{}, errors.Wrap(err, "failed to read audit retention setting")
	}
	retention := AuditRetention{MaxRecords: DefaultAuditMaxRecords, RetainDays: DefaultAuditRetainDays}
	if len(rows) > 0 && strings.TrimSpace(rows[0].Value) != "" {
		if err := json.Unmarshal([]byte(rows[0].Value), &retention); err != nil {
			return AuditRetention{}, errors.Wrap(err, "failed to parse audit retention setting")
		}
		if err := retention.Validate(); err != nil {
			return AuditRetention{}, err
		}
	}
	s.setAuditRetentionCached(retention)
	return retention, nil
}

// UpdateAuditRetention declares the retention policy and prunes the ledger to
// the new bounds immediately, so tightening the policy takes effect at once.
func (s *Store) UpdateAuditRetention(ctx context.Context, retention AuditRetention) error {
	if err := retention.Validate(); err != nil {
		return err
	}
	value, err := json.Marshal(retention)
	if err != nil {
		return errors.Wrap(err, "failed to marshal audit retention setting")
	}
	if _, err := s.driver.UpsertInstanceSetting(ctx, &InstanceSetting{
		Name:  auditRetentionSettingName,
		Value: string(value),
	}); err != nil {
		return errors.Wrap(err, "failed to write audit retention setting")
	}
	s.setAuditRetentionCached(retention)
	maxRecords, beforeSec := retention.Bounds(time.Now())
	if err := s.driver.PruneAuditLogs(ctx, maxRecords, beforeSec); err != nil {
		return errors.Wrap(err, "failed to prune audit logs to the new retention")
	}
	return nil
}

// currentAuditBounds loads the retention and converts it to pruning bounds.
// A read failure falls back to the defaults rather than blocking the
// business mutation: an audit write with default bounds beats no entry.
func (s *Store) currentAuditBounds(ctx context.Context) AuditRetention {
	retention, err := s.GetAuditRetention(ctx)
	if err != nil {
		return AuditRetention{MaxRecords: DefaultAuditMaxRecords, RetainDays: DefaultAuditRetainDays}
	}
	return retention
}

// ListAuditLogs returns ledger entries matching find, newest first.
func (s *Store) ListAuditLogs(ctx context.Context, find *FindAuditLog) ([]*AuditLog, error) {
	return s.driver.ListAuditLogs(ctx, find)
}

// CreateAuditLog writes an entry directly and prunes the ledger to the current
// retention in the same transaction. Business code uses the audited mutation
// methods instead; this exists for tests and administrative tooling.
func (s *Store) CreateAuditLog(ctx context.Context, create *AuditLog) error {
	if create.CreatedTs == 0 {
		create.CreatedTs = time.Now().Unix()
	}
	maxRecords, beforeSec := s.currentAuditBounds(ctx).Bounds(time.Now())
	return s.driver.CreateAuditLog(ctx, create, maxRecords, beforeSec)
}

// UpsertInstanceSettingWithAudit upserts an instance setting and writes its
// audit entry as one transaction. The cache is updated only after the
// transaction commits.
func (s *Store) UpsertInstanceSettingWithAudit(ctx context.Context, upsert *storepb.InstanceSetting, audit *AuditLog) (*storepb.InstanceSetting, error) {
	instanceSettingRaw := &InstanceSetting{Name: upsert.Key.String()}
	valueBytes, err := marshalInstanceSettingValue(upsert)
	if err != nil {
		return nil, err
	}
	instanceSettingRaw.Value = string(valueBytes)
	if audit.CreatedTs == 0 {
		audit.CreatedTs = time.Now().Unix()
	}
	maxRecords, beforeSec := s.currentAuditBounds(ctx).Bounds(time.Now())
	if err := s.driver.UpsertInstanceSettingAudited(ctx, instanceSettingRaw, audit, maxRecords, beforeSec); err != nil {
		return nil, errors.Wrap(err, "failed to upsert instance setting with audit entry")
	}
	instanceSetting, err := convertInstanceSettingFromRaw(instanceSettingRaw)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert instance setting")
	}
	s.cacheInstanceSetting(ctx, instanceSetting)
	if upsert.Key == storepb.InstanceSettingKey_STORAGE {
		s.resetStorageDriverCache()
	}
	return instanceSetting, nil
}

// CreateIdentityProviderWithAudit creates an identity provider and writes its
// audit entry as one transaction.
func (s *Store) CreateIdentityProviderWithAudit(ctx context.Context, create *storepb.IdentityProvider, audit *AuditLog) (*storepb.IdentityProvider, error) {
	raw, err := convertIdentityProviderToRaw(create)
	if err != nil {
		return nil, err
	}
	if audit.CreatedTs == 0 {
		audit.CreatedTs = time.Now().Unix()
	}
	maxRecords, beforeSec := s.currentAuditBounds(ctx).Bounds(time.Now())
	raw, err = s.driver.CreateIdentityProviderAudited(ctx, raw, audit, maxRecords, beforeSec)
	if err != nil {
		return nil, err
	}
	return convertIdentityProviderFromRaw(raw)
}

// UpdateIdentityProviderWithAudit updates an identity provider and writes its
// audit entry as one transaction.
func (s *Store) UpdateIdentityProviderWithAudit(ctx context.Context, update *UpdateIdentityProviderV1, audit *AuditLog) (*storepb.IdentityProvider, error) {
	updateRaw := &UpdateIdentityProvider{ID: update.ID}
	if update.Name != nil {
		updateRaw.Name = update.Name
	}
	if update.IdentifierFilter != nil {
		updateRaw.IdentifierFilter = update.IdentifierFilter
	}
	if update.Config != nil {
		configRaw, err := convertIdentityProviderConfigToRaw(update.Type, update.Config)
		if err != nil {
			return nil, err
		}
		updateRaw.Config = &configRaw
	}
	if audit.CreatedTs == 0 {
		audit.CreatedTs = time.Now().Unix()
	}
	maxRecords, beforeSec := s.currentAuditBounds(ctx).Bounds(time.Now())
	raw, err := s.driver.UpdateIdentityProviderAudited(ctx, updateRaw, audit, maxRecords, beforeSec)
	if err != nil {
		return nil, err
	}
	return convertIdentityProviderFromRaw(raw)
}

// DeleteIdentityProviderSafelyWithAudit validates and deletes an identity
// provider, writing its audit entry in the same serialized mutation.
func (s *Store) DeleteIdentityProviderSafelyWithAudit(ctx context.Context, delete *DeleteIdentityProvider, audit *AuditLog) error {
	if audit.CreatedTs == 0 {
		audit.CreatedTs = time.Now().Unix()
	}
	maxRecords, beforeSec := s.currentAuditBounds(ctx).Bounds(time.Now())
	mutation := &AuthenticationConfigMutation{
		DeleteIdentityProviderID: &delete.ID,
		Audit:                    audit,
		AuditMaxRecords:          maxRecords,
		AuditBeforeSec:            beforeSec,
		Validate: func(state *AuthenticationConfigState) error {
			return s.validateAuthenticationMutationState(state, nil, &delete.ID)
		},
	}
	return s.applyAuthenticationConfigMutation(ctx, mutation)
}

// UpsertInstanceGeneralSettingSafelyWithAudit validates and stores GENERAL as
// one serialized operation together with its audit entry.
func (s *Store) UpsertInstanceGeneralSettingSafelyWithAudit(ctx context.Context, setting *storepb.InstanceSetting, audit *AuditLog) (*storepb.InstanceSetting, error) {
	if setting == nil || setting.Key != storepb.InstanceSettingKey_GENERAL || setting.GetGeneralSetting() == nil {
		return nil, errors.New("GENERAL instance setting is required")
	}
	value, err := marshalInstanceSettingValue(setting)
	if err != nil {
		return nil, err
	}
	raw := &InstanceSetting{Name: storepb.InstanceSettingKey_GENERAL.String(), Value: string(value)}
	if audit.CreatedTs == 0 {
		audit.CreatedTs = time.Now().Unix()
	}
	maxRecords, beforeSec := s.currentAuditBounds(ctx).Bounds(time.Now())
	mutation := &AuthenticationConfigMutation{
		UpsertGeneralSetting: raw,
		Audit:                audit,
		AuditMaxRecords:      maxRecords,
		AuditBeforeSec:        beforeSec,
		Validate: func(state *AuthenticationConfigState) error {
			return s.validateAuthenticationMutationState(state, setting.GetGeneralSetting(), nil)
		},
	}
	if err := s.applyAuthenticationConfigMutation(ctx, mutation); err != nil {
		return nil, err
	}
	result := cloneInstanceSetting(setting)
	s.cacheInstanceSetting(ctx, result)
	return result, nil
}

// UpdateUserWithAudit updates a user and writes the audit entry in the same
// transaction. The user cache is updated only after the transaction commits.
func (s *Store) UpdateUserWithAudit(ctx context.Context, update *UpdateUser, audit *AuditLog) (*User, error) {
	if update.Email != nil {
		email, err := normalizeUserEmail(*update.Email)
		if err != nil {
			return nil, err
		}
		update.Email = &email
	}
	if audit.CreatedTs == 0 {
		audit.CreatedTs = time.Now().Unix()
	}
	maxRecords, beforeSec := s.currentAuditBounds(ctx).Bounds(time.Now())
	user, err := s.driver.UpdateUserAudited(ctx, update, audit, maxRecords, beforeSec)
	if err != nil {
		if uniqueErr := classifyUserUniqueViolation(err); uniqueErr != nil {
			return nil, uniqueErr
		}
		return nil, err
	}
	s.userCache.Set(ctx, userCacheKey(user.ID), user)
	return user, nil
}
