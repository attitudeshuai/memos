// Package protohash computes a stable digest of a protobuf message without
// buffering a serialized copy of it.
//
// The digest uses a canonical self-delimiting framing rather than the protobuf
// wire format: messages are sequences of fields in ascending field number
// order, terminated by an end marker, so bytes values can be streamed straight
// into the hash. This keeps hashing requests that embed large payloads (for
// example, file uploads) from doubling the request's memory footprint.
//
// The encoding is an implementation detail of this package. It is deterministic
// for equal messages across processes and is not intended to be exchanged with
// other systems.
package protohash

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"math"
	"slices"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// fieldKind disambiguates the value encoding that follows a field header.
type fieldKind byte

const (
	kindVarint fieldKind = iota + 1
	kindFixed32
	kindFixed64
	kindBytes
	kindMessage
)

// messageEnd terminates the field sequence of a message. Field numbers are
// always positive, so a zero varint is an unambiguous marker.
const messageEnd uint64 = 0

// Digest returns the hex-encoded SHA-256 digest of a canonical encoding of m.
// Fields are visited in ascending field number order. When skipTopLevelField
// is non-empty, a field with that name on the top-level message is excluded;
// fields with the same name on nested messages are still included. Passing an
// empty name hashes every field.
func Digest(m proto.Message, skipTopLevelField protoreflect.Name) (string, error) {
	if m == nil {
		return "", nil
	}
	msg := m.ProtoReflect()
	if !msg.IsValid() {
		return "", nil
	}
	h := sha256.New()
	if err := writeMessage(h, msg, skipTopLevelField); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// writeMessage writes every populated field of msg followed by the end marker.
func writeMessage(w io.Writer, msg protoreflect.Message, skipField protoreflect.Name) error {
	var err error
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Name() == skipField {
			return true
		}
		if fd.IsMap() {
			err = writeMapField(w, fd, v.Map())
		} else if fd.IsList() {
			err = writeListField(w, fd, v.List())
		} else {
			err = writeSingularField(w, fd.Number(), fd, v)
		}
		return err == nil
	})
	if err != nil {
		return err
	}
	return writeUvarint(w, messageEnd)
}

// writeSingularField writes the field header and the value of one field. num is
// passed separately so map entries can use their synthetic key/value numbers.
func writeSingularField(w io.Writer, num protoreflect.FieldNumber, fd protoreflect.FieldDescriptor, v protoreflect.Value) error {
	switch {
	case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
		if err := writeFieldHeader(w, num, kindMessage); err != nil {
			return err
		}
		return writeMessage(w, v.Message(), "")
	case fd.Kind() == protoreflect.BytesKind:
		if err := writeFieldHeader(w, num, kindBytes); err != nil {
			return err
		}
		raw := v.Bytes()
		if err := writeUvarint(w, uint64(len(raw))); err != nil {
			return err
		}
		_, err := w.Write(raw)
		return err
	case fd.Kind() == protoreflect.StringKind:
		if err := writeFieldHeader(w, num, kindBytes); err != nil {
			return err
		}
		raw := v.String()
		if err := writeUvarint(w, uint64(len(raw))); err != nil {
			return err
		}
		_, err := io.WriteString(w, raw)
		return err
	case fd.Kind() == protoreflect.Fixed64Kind:
		if err := writeFieldHeader(w, num, kindFixed64); err != nil {
			return err
		}
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], v.Uint())
		_, err := w.Write(buf[:])
		return err
	case fd.Kind() == protoreflect.Sfixed64Kind:
		if err := writeFieldHeader(w, num, kindFixed64); err != nil {
			return err
		}
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(v.Int()))
		_, err := w.Write(buf[:])
		return err
	case fd.Kind() == protoreflect.DoubleKind:
		if err := writeFieldHeader(w, num, kindFixed64); err != nil {
			return err
		}
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v.Float()))
		_, err := w.Write(buf[:])
		return err
	case fd.Kind() == protoreflect.Fixed32Kind:
		if err := writeFieldHeader(w, num, kindFixed32); err != nil {
			return err
		}
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(v.Uint()))
		_, err := w.Write(buf[:])
		return err
	case fd.Kind() == protoreflect.Sfixed32Kind:
		if err := writeFieldHeader(w, num, kindFixed32); err != nil {
			return err
		}
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(v.Int()))
		_, err := w.Write(buf[:])
		return err
	case fd.Kind() == protoreflect.FloatKind:
		if err := writeFieldHeader(w, num, kindFixed32); err != nil {
			return err
		}
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], math.Float32bits(float32(v.Float())))
		_, err := w.Write(buf[:])
		return err
	case fd.Kind() == protoreflect.EnumKind:
		if err := writeFieldHeader(w, num, kindVarint); err != nil {
			return err
		}
		return writeUvarint(w, uint64(v.Enum()))
	case fd.Kind() == protoreflect.Int32Kind || fd.Kind() == protoreflect.Int64Kind ||
		fd.Kind() == protoreflect.Sint32Kind || fd.Kind() == protoreflect.Sint64Kind:
		if err := writeFieldHeader(w, num, kindVarint); err != nil {
			return err
		}
		return writeUvarint(w, uint64(v.Int()))
	case fd.Kind() == protoreflect.BoolKind:
		if err := writeFieldHeader(w, num, kindVarint); err != nil {
			return err
		}
		if v.Bool() {
			return writeUvarint(w, 1)
		}
		return writeUvarint(w, 0)
	default:
		if err := writeFieldHeader(w, num, kindVarint); err != nil {
			return err
		}
		// The unsigned integer kinds are the remaining varint kinds.
		return writeUvarint(w, v.Uint())
	}
}

// writeListField emits each list element as a field with the same number. A
// packed wire encoding is unnecessary because the digest only has to be stable,
// not wire-compatible.
func writeListField(w io.Writer, fd protoreflect.FieldDescriptor, list protoreflect.List) error {
	for i := 0; i < list.Len(); i++ {
		if err := writeSingularField(w, fd.Number(), fd, list.Get(i)); err != nil {
			return err
		}
	}
	return nil
}

// mapEntry is one collected map key, retained for deterministic sorting.
type mapEntry struct {
	key   protoreflect.MapKey
	value protoreflect.Value
}

// writeMapField sorts entries by key and encodes each entry as a message with
// key field 1 and value field 2, matching the protobuf map representation.
func writeMapField(w io.Writer, fd protoreflect.FieldDescriptor, m protoreflect.Map) error {
	entries := make([]mapEntry, 0, m.Len())
	m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		entries = append(entries, mapEntry{key: k, value: v})
		return true
	})
	slices.SortFunc(entries, func(x, y mapEntry) int {
		return compareMapKeys(fd.MapKey().Kind(), x.key, y.key)
	})

	keyFD := fd.MapKey()
	valueFD := fd.MapValue()
	for _, entry := range entries {
		if err := writeFieldHeader(w, fd.Number(), kindMessage); err != nil {
			return err
		}
		if err := writeSingularField(w, 1, syntheticField(keyFD, 1), entry.key.Value()); err != nil {
			return err
		}
		if err := writeSingularField(w, 2, syntheticField(valueFD, 2), entry.value); err != nil {
			return err
		}
		if err := writeUvarint(w, messageEnd); err != nil {
			return err
		}
	}
	return nil
}

// compareMapKeys orders map keys deterministically for every legal key kind.
func compareMapKeys(kind protoreflect.Kind, a, b protoreflect.MapKey) int {
	switch kind {
	case protoreflect.BoolKind:
		// false sorts before true.
		if a.Bool() == b.Bool() {
			return 0
		}
		if !a.Bool() {
			return -1
		}
		return 1
	case protoreflect.StringKind:
		switch {
		case a.String() < b.String():
			return -1
		case a.String() > b.String():
			return 1
		default:
			return 0
		}
	default:
		// Signed and unsigned integer kinds must be compared separately.
		if kind == protoreflect.Int32Kind || kind == protoreflect.Int64Kind ||
			kind == protoreflect.Sint32Kind || kind == protoreflect.Sint64Kind ||
			kind == protoreflect.Sfixed32Kind || kind == protoreflect.Sfixed64Kind {
			ai, bi := a.Int(), b.Int()
			switch {
			case ai < bi:
				return -1
			case ai > bi:
				return 1
			default:
				return 0
			}
		}
		au, bu := a.Uint(), b.Uint()
		switch {
		case au < bu:
			return -1
		case au > bu:
			return 1
		default:
			return 0
		}
	}
}

// syntheticFieldDescriptor wraps a field descriptor replacing only its number,
// which map entry fields need because their keys and values use numbers 1 and 2
// regardless of the enclosing field.
type syntheticFieldDescriptor struct {
	protoreflect.FieldDescriptor
	number protoreflect.FieldNumber
}

func (s syntheticFieldDescriptor) Number() protoreflect.FieldNumber { return s.number }

func syntheticField(fd protoreflect.FieldDescriptor, number protoreflect.FieldNumber) protoreflect.FieldDescriptor {
	if fd.Number() == number {
		return fd
	}
	return syntheticFieldDescriptor{FieldDescriptor: fd, number: number}
}

func writeFieldHeader(w io.Writer, num protoreflect.FieldNumber, kind fieldKind) error {
	if err := writeUvarint(w, uint64(num)); err != nil {
		return err
	}
	return writeUvarint(w, uint64(kind))
}

func writeUvarint(w io.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := w.Write(buf[:n])
	return err
}
