package audit

import (
	"bytes"
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	storepb "github.com/usememos/memos/proto/gen/store"
)

// redactedValue replaces a non-empty sensitive value. The marker reveals
// whether a secret was present or changed without disclosing it.
const redactedValue = "***"

// sensitiveSegments are the field-path fragments that carry secrets. A path is
// masked when any of its dot-separated segments matches one, case
// insensitively. "access_key_id" is intentionally absent: it is an
// identifier, not a secret.
var sensitiveSegments = []string{
	"secret",
	"password",
	"token",
	"api_key",
	"apikey",
}

// diffMarshaler emits every field, so absent and default values compare
// consistently instead of producing false changes against unpopulated sides.
var diffMarshaler = protojson.MarshalOptions{
	EmitUnpopulated: true,
}

// MessageChanges returns the leaf field paths whose values differ between
// before and after, masking sensitive values. A nil message is treated as an
// empty message, which records every populated field of the other side as a
// change (used for create and delete).
func MessageChanges(before, after proto.Message) ([]FieldChange, error) {
	beforeTree, err := toTree(before)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read pre-change value")
	}
	afterTree, err := toTree(after)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read post-change value")
	}
	changes := []FieldChange{}
	collectChanges("", beforeTree, afterTree, &changes)
	return changes, nil
}

// InstanceSettingPayload returns the oneof payload message of an instance
// setting (the typed value, without the key), or nil when no value is set.
func InstanceSettingPayload(setting *storepb.InstanceSetting) proto.Message {
	if setting == nil {
		return nil
	}
	message := setting.ProtoReflect()
	oneof := message.Descriptor().Oneofs().ByName("value")
	if oneof == nil {
		return nil
	}
	field := message.WhichOneof(oneof)
	if field == nil {
		return nil
	}
	return message.Get(field).Message().Interface()
}

func toTree(message proto.Message) (any, error) {
	if message == nil {
		return nil, nil
	}
	raw, err := diffMarshaler.Marshal(message)
	if err != nil {
		return nil, err
	}
	var tree any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&tree); err != nil {
		return nil, err
	}
	return tree, nil
}

func collectChanges(path string, before, after any, changes *[]FieldChange) {
	beforeMap, beforeIsMap := before.(map[string]any)
	afterMap, afterIsMap := after.(map[string]any)
	if beforeIsMap || afterIsMap {
		collectMapChanges(path, beforeMap, afterMap, changes)
		return
	}

	beforeList, beforeIsList := before.([]any)
	afterList, afterIsList := after.([]any)
	if beforeIsList || afterIsList {
		collectListChanges(path, beforeList, afterList, changes)
		return
	}

	appendLeafChange(path, before, after, changes)
}

func collectMapChanges(path string, before, after map[string]any, changes *[]FieldChange) {
	keys := make(map[string]struct{}, len(before)+len(after))
	for key := range before {
		keys[key] = struct{}{}
	}
	for key := range after {
		keys[key] = struct{}{}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	slices.Sort(ordered)
	for _, key := range ordered {
		childPath := key
		if path != "" {
			childPath = path + "." + key
		}
		collectChanges(childPath, before[key], after[key], changes)
	}
}

func collectListChanges(path string, before, after []any, changes *[]FieldChange) {
	if len(before) == 0 && len(after) == 0 {
		return
	}
	// Element-wise comparison stays readable for ordered lists of objects
	// such as storage entries, including added and removed elements; anything
	// else renders the whole list.
	if listsOfMaps(before) && listsOfMaps(after) {
		count := max(len(before), len(after))
		for index := range count {
			childPath := path + "[" + strconv.Itoa(index) + "]"
			var beforeElement, afterElement any
			if index < len(before) {
				beforeElement = before[index]
			}
			if index < len(after) {
				afterElement = after[index]
			}
			collectChanges(childPath, beforeElement, afterElement, changes)
		}
		return
	}
	if jsonValuesEqual(before, after) {
		return
	}
	appendLeafChange(path, before, after, changes)
}

func listsOfMaps(values []any) bool {
	for _, value := range values {
		if _, ok := value.(map[string]any); !ok {
			return false
		}
	}
	return true
}

func appendLeafChange(path string, before, after any, changes *[]FieldChange) {
	if path == "" {
		return
	}
	beforeValue := renderJSONValue(before)
	afterValue := renderJSONValue(after)
	if leafValuesEqual(before, after, beforeValue, afterValue) {
		return
	}
	if isSensitivePath(path) {
		beforeValue = maskValue(beforeValue)
		afterValue = maskValue(afterValue)
	}
	*changes = append(*changes, FieldChange{Field: path, Before: beforeValue, After: afterValue})
}

// leafValuesEqual compares rendered values while treating a missing value
// (JSON null) and an explicit proto3 zero as equal, so an unset submessage
// does not log every default-valued field when it is first populated.
func leafValuesEqual(before, after any, beforeValue, afterValue string) bool {
	if beforeValue == afterValue {
		return true
	}
	return (before == nil && isZeroScalar(afterValue)) || (after == nil && isZeroScalar(beforeValue))
}

func isZeroScalar(rendered string) bool {
	return rendered == "" || rendered == "false" || rendered == "0"
}

func isSensitivePath(path string) bool {
	for _, segment := range strings.FieldsFunc(path, func(r rune) bool {
		return r == '.' || r == '[' || r == ']'
	}) {
		lower := strings.ToLower(segment)
		for _, keyword := range sensitiveSegments {
			if strings.Contains(lower, keyword) {
				return true
			}
		}
	}
	return false
}

// maskValue keeps an empty value empty and hides every non-empty value, so a
// diff still shows that a secret was added, changed, or removed.
func maskValue(value string) string {
	if value == "" {
		return ""
	}
	return redactedValue
}

func renderJSONValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case json.Number:
		return typed.String()
	case bool:
		return strconv.FormatBool(typed)
	default:
		raw, err := json.Marshal(typed)
		if err != nil {
			return ""
		}
		return string(raw)
	}
}

func jsonValuesEqual(before, after any) bool {
	if (before == nil) != (after == nil) {
		return false
	}
	beforeRaw, err := json.Marshal(before)
	if err != nil {
		return false
	}
	afterRaw, err := json.Marshal(after)
	if err != nil {
		return false
	}
	return bytes.Equal(beforeRaw, afterRaw)
}
