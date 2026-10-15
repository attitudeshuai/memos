package store

import (
	"fmt"
	"strings"

	"github.com/usememos/memos/filter"
)

// MemoKeyset is a keyset pagination position for FindMemo. Its values are the
// sort-key values of the previous page's last row, interpreted under
// FindMemo's ordering flags. When FindMemo.Cursor is set, FindMemo.Offset is
// ignored.
//
// All sort-key columns are NOT NULL, so the keyset defines a strict total
// order: pinned (optional, DESC), created_ts or updated_ts (ASC or DESC), and
// id (DESC, final tie-breaker).
type MemoKeyset struct {
	// LastPinned is the pinned value of the previous page's last row. Only
	// read when FindMemo.OrderByPinned is set.
	LastPinned bool
	// LastTs is the created_ts (or updated_ts when OrderByUpdatedTs is set)
	// value of the previous page's last row, in Unix seconds.
	LastTs int64
	// LastID is the id tie-breaker value of the previous page's last row.
	LastID int32
}

// keysetKey is one sort key participating in the keyset position.
type keysetKey struct {
	column string
	desc   bool
	value  any
}

// AppendMemoKeysetCondition appends a WHERE predicate that selects rows
// strictly after find.Cursor in the ordering defined by FindMemo's ordering
// flags. It renders identically ordered predicates for every supported
// dialect, so keyset pagination returns the same row sequence on SQLite,
// MySQL, and PostgreSQL.
//
// For ORDER BY k1 d1, k2 d2, ... with cursor values v1, v2, ... the predicate
// is:
//
//	(k1 <op1> v1)
//	  OR (k1 = v1 AND k2 <op2> v2)
//	  OR (k1 = v1 AND k2 = v2 AND k3 <op3> v3)
//
// where <op> is "<" for DESC keys and ">" for ASC keys. Mixed key directions
// (for example time ASC with the id DESC tie-breaker) compose correctly.
func AppendMemoKeysetCondition(find *FindMemo, dialect filter.DialectName, where *[]string, args *[]any) error {
	if find.Cursor == nil {
		return nil
	}

	keys, err := memoKeysetKeys(find, dialect)
	if err != nil {
		return err
	}

	clauses := make([]string, 0, len(keys))
	for i := range keys {
		terms := make([]string, 0, i+1)
		// Earlier keys must equal the cursor row's values.
		for k := range i {
			ph, err := appendKeysetPlaceholder(dialect, args, keys[k].value)
			if err != nil {
				return err
			}
			terms = append(terms, keys[k].column+" = "+ph)
		}
		// The current key must be strictly after the cursor row.
		comparator := "<"
		if !keys[i].desc {
			comparator = ">"
		}
		ph, err := appendKeysetPlaceholder(dialect, args, keys[i].value)
		if err != nil {
			return err
		}
		terms = append(terms, keys[i].column+" "+comparator+" "+ph)
		clauses = append(clauses, "("+strings.Join(terms, " AND ")+")")
	}

	*where = append(*where, "("+strings.Join(clauses, " OR ")+")")
	return nil
}

// memoKeysetKeys derives the sort keys in the same order as the drivers'
// ORDER BY clauses.
func memoKeysetKeys(find *FindMemo, dialect filter.DialectName) ([]keysetKey, error) {
	boolValue := func(v bool) (any, error) {
		switch dialect {
		case filter.DialectPostgres:
			return v, nil
		case filter.DialectSQLite, filter.DialectMySQL:
			if v {
				return int64(1), nil
			}
			return int64(0), nil
		default:
			return nil, fmt.Errorf("unsupported dialect %q for memo keyset", dialect)
		}
	}

	keys := make([]keysetKey, 0, 3)
	if find.OrderByPinned {
		pinned, err := boolValue(find.Cursor.LastPinned)
		if err != nil {
			return nil, err
		}
		keys = append(keys, keysetKey{
			column: memoKeysetColumn(dialect, "pinned"),
			desc:   true,
			value:  pinned,
		})
	}

	timeColumn := "created_ts"
	if find.OrderByUpdatedTs {
		timeColumn = "updated_ts"
	}
	keys = append(keys, keysetKey{
		column: memoKeysetColumn(dialect, timeColumn),
		desc:   !find.OrderByTimeAsc,
		value:  find.Cursor.LastTs,
	})

	// id is the final tie-breaker in every driver's ORDER BY and always sorts
	// DESC.
	keys = append(keys, keysetKey{
		column: memoKeysetColumn(dialect, "id"),
		desc:   true,
		value:  find.Cursor.LastID,
	})
	return keys, nil
}

// memoKeysetColumn renders a qualified memo column. MySQL stores timestamps as
// TIMESTAMP values, so they are compared through UNIX_TIMESTAMP against Unix
// second arguments, matching the filter engine and the SELECT projections.
func memoKeysetColumn(dialect filter.DialectName, name string) string {
	switch dialect {
	case filter.DialectPostgres:
		return "memo." + name
	case filter.DialectSQLite, filter.DialectMySQL:
		column := "`memo`.`" + name + "`"
		if dialect == filter.DialectMySQL && (name == "created_ts" || name == "updated_ts") {
			return "UNIX_TIMESTAMP(" + column + ")"
		}
		return column
	default:
		return name
	}
}

// appendKeysetPlaceholder registers a bound value and returns its placeholder
// for the dialect.
func appendKeysetPlaceholder(dialect filter.DialectName, args *[]any, value any) (string, error) {
	switch dialect {
	case filter.DialectPostgres:
		*args = append(*args, value)
		return fmt.Sprintf("$%d", len(*args)), nil
	case filter.DialectSQLite, filter.DialectMySQL:
		*args = append(*args, value)
		return "?", nil
	default:
		return "", fmt.Errorf("unsupported dialect %q for memo keyset", dialect)
	}
}
