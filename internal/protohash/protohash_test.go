package protohash

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestDigestStableAndDistinct(t *testing.T) {
	t.Parallel()

	first := timestamppb.New(dateTime(2021, 6, 15, 10, 30))
	second := proto.Clone(first)

	d1, err := Digest(first, "")
	requireNoError(t, err)
	d2, err := Digest(second, "")
	requireNoError(t, err)
	if d1 != d2 {
		t.Fatalf("equal messages must hash equally: %q vs %q", d1, d2)
	}
	if len(d1) != 64 {
		t.Fatalf("digest must be hex-encoded SHA-256 (64 chars), got %d", len(d1))
	}

	other := timestamppb.New(dateTime(2021, 6, 15, 10, 31))
	d3, err := Digest(other, "")
	requireNoError(t, err)
	if d1 == d3 {
		t.Fatal("messages that differ in a field must hash differently")
	}
}

func TestDigestDistinguishesEveryScalarKind(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		a    proto.Message
		b    proto.Message
	}{
		{"int32", wrapperspb.Int32(-123), wrapperspb.Int32(-124)},
		{"int64", wrapperspb.Int64(-1234567890), wrapperspb.Int64(-1234567891)},
		{"uint32", wrapperspb.UInt32(123), wrapperspb.UInt32(124)},
		{"uint64", wrapperspb.UInt64(1234567890), wrapperspb.UInt64(1234567891)},
		{"bool", wrapperspb.Bool(false), wrapperspb.Bool(true)},
		{"float", wrapperspb.Float(1.5), wrapperspb.Float(1.75)},
		{"double", wrapperspb.Double(1.5), wrapperspb.Double(1.75)},
		{"string", wrapperspb.String("a"), wrapperspb.String("b")},
		{"bytes", wrapperspb.Bytes([]byte{1, 2, 3}), wrapperspb.Bytes([]byte{1, 2, 4})},
		{"message", timestamppb.New(dateTime(2021, 1, 1, 0, 0)), timestamppb.New(dateTime(2022, 1, 1, 0, 0))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			da, err := Digest(tc.a, "")
			requireNoError(t, err)
			db, err := Digest(tc.b, "")
			requireNoError(t, err)
			if da == db {
				t.Fatal("different payloads must produce different digests")
			}
		})
	}
}

func TestDigestStableWithLargeBytes(t *testing.T) {
	t.Parallel()

	// A large bytes field must hash without buffering a serialized copy;
	// equality across calls is the contract that lets upload retries match
	// their leader.
	big := make([]byte, 4*1024*1024)
	for i := range big {
		big[i] = byte(i % 251)
	}
	a := wrapperspb.Bytes(big)
	b := wrapperspb.Bytes(append([]byte(nil), big...))

	da, err := Digest(a, "")
	requireNoError(t, err)
	db, err := Digest(b, "")
	requireNoError(t, err)
	if da != db {
		t.Fatal("equal multi-megabyte payloads must hash equally")
	}
}

func TestDigestSortsMapEntries(t *testing.T) {
	t.Parallel()

	m1 := structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
		"a": structpb.NewStringValue("1"),
		"b": structpb.NewStringValue("2"),
	}})
	m2 := structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
		"b": structpb.NewStringValue("2"),
		"a": structpb.NewStringValue("1"),
	}})

	d1, err := Digest(m1, "")
	requireNoError(t, err)
	d2, err := Digest(m2, "")
	requireNoError(t, err)
	if d1 != d2 {
		t.Fatalf("map entries must be order-independent: %q vs %q", d1, d2)
	}

	m3 := structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
		"a": structpb.NewStringValue("1"),
		"b": structpb.NewStringValue("3"),
	}})
	d3, err := Digest(m3, "")
	requireNoError(t, err)
	if d1 == d3 {
		t.Fatal("different map values must change the digest")
	}
}

func TestDigestRepeatedAndOneofOrdering(t *testing.T) {
	t.Parallel()

	// structpb.Value is a oneof and ListValue carries a repeated message.
	tags := func(values ...string) *structpb.Value {
		list := &structpb.ListValue{Values: make([]*structpb.Value, 0, len(values))}
		for _, v := range values {
			list.Values = append(list.Values, structpb.NewStringValue(v))
		}
		return structpb.NewListValue(list)
	}

	da, err := Digest(tags("a", "b"), "")
	requireNoError(t, err)
	db, err := Digest(tags("a", "b"), "")
	requireNoError(t, err)
	if da != db {
		t.Fatal("equal repeated payloads must hash equally")
	}
	dc, err := Digest(tags("b", "a"), "")
	requireNoError(t, err)
	if da == dc {
		t.Fatal("repeated field order is part of the payload")
	}
}

func TestDigestEnumKind(t *testing.T) {
	t.Parallel()

	// Enum values are their own protoreflect kind and must not panic on Uint().
	a := newEnumMessage(t, "KIND_ON")
	b := newEnumMessage(t, "KIND_ON")
	c := newEnumMessage(t, "KIND_OFF")

	da, err := Digest(a, "")
	requireNoError(t, err)
	db, err := Digest(b, "")
	requireNoError(t, err)
	if da != db {
		t.Fatal("equal enum values must hash equally")
	}
	dc, err := Digest(c, "")
	requireNoError(t, err)
	if da == dc {
		t.Fatal("different enum values must hash differently")
	}
}

func TestDigestNilMessage(t *testing.T) {
	t.Parallel()

	var m *timestamppb.Timestamp
	d, err := Digest(m, "")
	requireNoError(t, err)
	if d != "" {
		t.Fatalf("nil message digest must be empty, got %q", d)
	}
}

// newEnumMessage builds a dynamic message with one enum field so the digest
// can be exercised without importing generated application protos.
func newEnumMessage(t *testing.T, value protoreflect.Name) proto.Message {
	t.Helper()
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:    new("protohash_test.proto"),
		Package: new("protohash.test"),
		Syntax:  new("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: new("Kind"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: new("KIND_UNSPECIFIED"), Number: proto.Int32(0)},
				{Name: new("KIND_ON"), Number: proto.Int32(1)},
				{Name: new("KIND_OFF"), Number: proto.Int32(2)},
			},
		}},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: new("Envelope"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     new("kind"),
				JsonName: new("kind"),
				Number:   proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(),
				TypeName: new(".protohash.test.Kind"),
			}},
		}},
	}
	file, err := protodesc.NewFile(fileProto, nil)
	requireNoError(t, err)
	descriptor := file.Messages().ByName("Envelope")
	enum := file.Enums().ByName("Kind")
	message := dynamicpb.NewMessage(descriptor)
	enumValue := enum.Values().ByName(value)
	if enumValue == nil {
		t.Fatalf("test enum has no value %q", value)
	}
	message.Set(descriptor.Fields().ByName("kind"), protoreflect.ValueOfEnum(enumValue.Number()))
	return message
}

func dateTime(year, month, day, hour, minute int) time.Time {
	return time.Date(year, time.Month(month), day, hour, minute, 0, 0, time.UTC)
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
