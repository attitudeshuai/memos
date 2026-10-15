package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	apipb "github.com/usememos/memos/proto/gen/api"
	"github.com/usememos/memos/store"
)

const (
	// DefaultPageSize is the default page size for requests.
	DefaultPageSize = 50
	// MaxPageSize is the maximum page size for requests.
	MaxPageSize = 1000
)

// pageTokenSignatureDomain separates page-token HMACs from other HMAC uses of
// the same instance secret.
const pageTokenSignatureDomain = "memos.api.page_token.v1"

// deterministicPageTokenMarshal serializes a page token in a stable form so
// that signatures are reproducible.
var deterministicPageTokenMarshal = proto.MarshalOptions{Deterministic: true}

func convertStateFromStore(rowStatus store.RowStatus) apipb.State {
	switch rowStatus {
	case store.Normal:
		return apipb.State_NORMAL
	case store.Archived:
		return apipb.State_ARCHIVED
	default:
		return apipb.State_STATE_UNSPECIFIED
	}
}

func convertStateToStore(state apipb.State) store.RowStatus {
	switch state {
	case apipb.State_ARCHIVED:
		return store.Archived
	default:
		return store.Normal
	}
}

func getPageToken(limit int, offset int) (string, error) {
	return marshalPageToken(&apipb.PageToken{
		Limit:  int32(limit),
		Offset: int32(offset),
	})
}

func normalizePageSize(pageSize int32) int {
	limit := int(pageSize)
	if limit <= 0 {
		return DefaultPageSize
	}
	if limit > MaxPageSize {
		return MaxPageSize
	}
	return limit
}

func marshalPageToken(pageToken *apipb.PageToken) (string, error) {
	b, err := proto.Marshal(pageToken)
	if err != nil {
		return "", errors.Wrapf(err, "failed to marshal page token")
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func unmarshalPageToken(s string, pageToken *apipb.PageToken) error {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return errors.Wrapf(err, "failed to decode page token")
	}
	if err := proto.Unmarshal(b, pageToken); err != nil {
		return errors.Wrapf(err, "failed to unmarshal page token")
	}
	return nil
}

// pageTokenSignature computes the HMAC-SHA256 over the deterministic
// serialization of every page-token field except signature.
func pageTokenSignature(pageToken *apipb.PageToken, secret string) ([]byte, error) {
	unsigned := proto.Clone(pageToken).(*apipb.PageToken)
	unsigned.Signature = nil
	payload, err := deterministicPageTokenMarshal.Marshal(unsigned)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal page token for signing")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(pageTokenSignatureDomain))
	_, _ = mac.Write(payload)
	return mac.Sum(nil), nil
}

// signPageToken populates the token's signature field.
func signPageToken(pageToken *apipb.PageToken, secret string) error {
	signature, err := pageTokenSignature(pageToken, secret)
	if err != nil {
		return err
	}
	pageToken.Signature = signature
	return nil
}

// verifyPageTokenSignature reports whether the token carries a valid, untampered
// signature.
func verifyPageTokenSignature(pageToken *apipb.PageToken, secret string) bool {
	expected, err := pageTokenSignature(pageToken, secret)
	if err != nil {
		return false
	}
	return hmac.Equal(expected, pageToken.Signature)
}
