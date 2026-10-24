package s3

import (
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	zstar "github.com/brian14708/zstar/go"
)

// PackClient combines minimal native read and multipart upload capabilities.
type PackClient interface {
	SourceClient
	MultipartClient
}

// PackStore stores immutable content-addressed artifacts and conditional ranges.
type PackStore struct {
	client         PackClient
	bucket, prefix string
	options        UploadOptions
	policyMu       sync.Mutex
	policies       map[string]zstar.ContentCachePolicy
}

// NewPackStore binds native S3 configuration and bounded immutable uploads.
func NewPackStore(client PackClient, bucket, prefix string, options UploadOptions) *PackStore {
	options.Immutable = true
	return &PackStore{client: client, bucket: bucket, prefix: strings.TrimSuffix(prefix, "/"), options: options, policies: make(map[string]zstar.ContentCachePolicy)}
}

func (s *PackStore) key(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

// PackCacheAllowed reports admission from the last successful packed range.
func (s *PackStore) PackCacheAllowed(snapshot zstar.PackSnapshot) bool {
	s.policyMu.Lock()
	defer s.policyMu.Unlock()
	policy, ok := s.policies[snapshot.Key]
	return ok && policy.AllowsReuse()
}

// PutPack publishes an immutable artifact and captures its completed identity.
func (s *PackStore) PutPack(ctx context.Context, key string, input io.Reader, length uint64) (zstar.PackSnapshot, error) {
	var out zstar.PackSnapshot
	if s.client == nil || length == 0 || length > math.MaxInt64 {
		return out, errors.New("zstar s3: invalid pack input")
	}
	err := Upload(ctx, s.client, s.bucket, s.key(key), input, s.options)
	if err != nil && !errors.Is(err, errExists) {
		return out, err
	}
	meta, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.key(key))})
	if err != nil {
		return out, redacted(err)
	}
	if meta == nil || meta.ContentLength == nil || *meta.ContentLength < 0 || uint64(*meta.ContentLength) != length {
		return out, &zstar.FormatError{Category: "changed-pack", Cause: errors.New("published pack length")}
	}
	out = zstar.PackSnapshot{Key: key, Length: length, Version: aws.ToString(meta.VersionId), ETag: aws.ToString(meta.ETag)}
	return out, out.Validate()
}

// ReadPackedRange pins the catalog identity without a discovery HEAD.
func (s *PackStore) ReadPackedRange(ctx context.Context, snapshot zstar.PackSnapshot, offset, length uint64) ([]byte, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	if snapshot.Length > math.MaxInt64 || offset > snapshot.Length || length > snapshot.Length-offset {
		return nil, errors.New("zstar s3: invalid packed range")
	}
	source := NewSource(s.client, s.bucket, s.key(snapshot.Key))
	source.known = true
	source.size = int64(snapshot.Length)
	if snapshot.ETag != "" {
		source.etag = aws.String(snapshot.ETag)
	}
	if snapshot.Version != "" && snapshot.Version != "null" {
		source.version = aws.String(snapshot.Version)
	}
	data, err := source.ReadRange(ctx, int64(offset), int64(length))
	if errors.Is(err, errObjectChanged) {
		return nil, &zstar.FormatError{Category: "changed-pack", Cause: err}
	}
	if err == nil {
		s.policyMu.Lock()
		if len(s.policies) >= 1024 {
			for key := range s.policies {
				delete(s.policies, key)
				break
			}
		}
		policy := s.policies[snapshot.Key]
		policy.Observe(source.policy)
		s.policies[snapshot.Key] = policy
		s.policyMu.Unlock()
	}
	return data, err
}

var errExists = errors.New("zstar s3: immutable object exists")

func immutableError(err error) error {
	if api, ok := errors.AsType[smithy.APIError](err); ok && api.ErrorCode() == "PreconditionFailed" {
		return errExists
	}
	return redacted(err)
}
