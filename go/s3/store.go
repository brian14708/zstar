// Package s3 provides native AWS SDK v2 adapters for S3-compatible Zstar storage.
// Credentials, endpoints, and object keys are adapter policy, never wire fields.
package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/bits"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	zstar "github.com/brian14708/zstar/go"
	"github.com/klauspost/compress/zstd"
)

// SourceClient provides the S3 operations needed to read an immutable source.
type SourceClient interface {
	HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error)
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
}

// BlobClient provides the S3 operations needed to read and write external objects.
type BlobClient interface {
	SourceClient
	PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
}

// ErrNotFound identifies a missing S3 object through errors.Is.
var ErrNotFound = errors.New("zstar s3: object not found")

func redacted(err error) error {
	if errors.Is(err, ErrNotFound) {
		return ErrNotFound
	}
	if errors.Is(err, fs.ErrClosed) {
		return fs.ErrClosed
	}

	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if api, ok := errors.AsType[smithy.APIError](err); ok {
		switch api.ErrorCode() {
		case "NoSuchKey", "NotFound", "NoSuchBucket":
			return ErrNotFound
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
			return errors.New("zstar s3: authorization failed")
		case "PreconditionFailed":
			return errObjectChanged
		}
	}
	return errors.New("zstar s3: storage request failed")
}

// Source pins size and ETag/version from the first HEAD, then performs exact
// conditional ranges. Reuse it for one immutable archive reader.
type Source struct {
	client                       SourceClient
	bucket, key                  string
	maxRange                     int64
	mu                           sync.Mutex
	known                        bool
	size                         int64
	etag, version                *string
	manifestOffset, manifestSize uint64
	hasManifest                  bool
}

// NewSource creates a read-only source that pins size and ETag or version on
// first use. Configure its range budget before concurrent reads.
func NewSource(client SourceClient, bucket, key string) *Source {
	return &Source{client: client, bucket: bucket, key: key, maxRange: 256<<20 + 8}
}

// WithMaxRangeBytes configures the source before use; it is not concurrency safe.
func (s *Source) WithMaxRangeBytes(limit int64) *Source { s.maxRange = limit; return s }

// Size pins and returns the immutable object length using a credentialed HEAD.
func (s *Source) Size(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.known {
		return s.size, nil
	}
	if s.client == nil {
		return 0, errors.New("zstar s3: nil client")
	}
	meta, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.key)})
	if err != nil {
		return 0, redacted(err)
	}
	if meta == nil || meta.ContentLength == nil || *meta.ContentLength < 0 {
		return 0, errors.New("zstar s3: invalid object metadata")
	}
	s.size = *meta.ContentLength
	s.etag = meta.ETag
	s.version = meta.VersionId
	if aws.ToString(s.etag) == "" && (aws.ToString(s.version) == "" || aws.ToString(s.version) == "null") {
		return 0, errors.New("zstar s3: object has no immutable identity")
	}
	s.manifestOffset, s.manifestSize, s.hasManifest = parseManifestPosition(meta.Metadata)
	s.known = true
	return s.size, nil
}

// ManifestRange returns the location hint from the same pinned HEAD as Size.
func (s *Source) ManifestRange(ctx context.Context) (offset, encodedSize uint64, present bool, err error) {
	size, err := s.Size(ctx)
	if err != nil {
		return 0, 0, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	usable := s.hasManifest && s.manifestOffset <= uint64(size) &&
		uint64(size)-s.manifestOffset <= uint64(max(s.maxRange, 0))
	return s.manifestOffset, s.manifestSize, usable, nil
}

// ReadRange returns an exact conditional range from the pinned object.
// ctx bounds the request; backend diagnostics are redacted.
func (s *Source) ReadRange(ctx context.Context, offset, length int64) (data []byte, err error) {
	if offset < 0 || length < 0 {
		return nil, errors.New("zstar s3: negative range")
	}
	size, err := s.Size(ctx)
	if err != nil {
		return nil, err
	}
	if offset > size || length > size-offset {
		return nil, errors.New("zstar s3: range outside object")
	}
	if length == 0 {
		return []byte{}, nil
	}
	if length > s.maxRange || uint64(length) >= uint64(math.MaxInt) {
		return nil, errors.New("zstar s3: local range budget")
	}
	result, err := s.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.key), Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)), IfMatch: s.etag, VersionId: s.version})
	if err != nil {
		return nil, redacted(err)
	}
	if result == nil || result.Body == nil {
		return nil, errors.New("zstar s3: missing response body")
	}
	defer func() {
		if closeErr := result.Body.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("zstar s3: close response: %w", redacted(closeErr)))
			data = nil
		}
	}()
	expected := fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, size)
	if aws.ToString(result.ContentRange) != expected || result.ContentLength == nil || *result.ContentLength != length || (s.etag != nil && (result.ETag == nil || aws.ToString(result.ETag) != aws.ToString(s.etag))) || (s.version != nil && (result.VersionId == nil || aws.ToString(result.VersionId) != aws.ToString(s.version))) {
		return nil, errObjectChanged
	}
	raw, err := readExact(result.Body, int(length))
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return raw, nil
}

func readExact(reader io.Reader, count int) ([]byte, error) {
	if count < 0 || count == math.MaxInt {
		return nil, errors.New("zstar s3: local range budget")
	}
	raw := make([]byte, count+1)
	n, err := io.ReadFull(reader, raw)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, redacted(err)
	}
	if n < count {
		return nil, fmt.Errorf("zstar s3: incomplete response: %w", io.ErrUnexpectedEOF)
	}
	if n > count {
		return nil, errors.New("zstar s3: oversized response")
	}
	return raw[:count], nil
}

// BlobStoreOptions controls object key policy and bounded request concurrency.
type BlobStoreOptions struct {
	Prefix string
	// CheckExists checks HEAD before compression and upload. Enable for workloads
	// with repeated content; new objects incur one additional request.
	CheckExists bool
	// Key maps raw digest to storage key. It overrides Prefix when nonnil.
	Key         func([32]byte) string
	Concurrency int // zero selects eight
}

// BlobStore reads and writes independent compressed content-addressed objects.
// Its methods and caller-supplied key function must support concurrent calls.
type BlobStore struct {
	client      BlobClient
	bucket      string
	key         func([32]byte) string
	slots       chan struct{}
	done        chan struct{}
	checkExists bool
	mu          sync.Mutex
	encoder     *objectEncoder
}

// An idle workspace travels as one value; checked-out codecs are operation-owned.
type objectEncoder struct {
	codec  *zstd.Encoder
	window int
	output []byte
}

// NewBlobStore creates a reusable bounded external object resolver and uploader.
// Zero concurrency selects eight; the caller owns the SDK client.
func NewBlobStore(client BlobClient, bucket string, options BlobStoreOptions) (*BlobStore, error) {
	if options.Concurrency == 0 {
		options.Concurrency = 8
	}
	if client == nil || options.Concurrency < 1 || options.Concurrency > 1024 {
		return nil, errors.New("zstar s3: invalid blob options")
	}
	key := options.Key
	if key == nil {
		prefix := strings.TrimSuffix(options.Prefix, "/")
		key = func(digest [32]byte) string {
			suffix := hex.EncodeToString(digest[:]) + ".zst"
			if prefix == "" {
				return suffix
			}
			return prefix + "/" + suffix
		}
	}
	return &BlobStore{client: client, bucket: bucket, key: key, slots: make(chan struct{}, options.Concurrency), done: make(chan struct{}), checkExists: options.CheckExists}, nil
}

// ObjectKey maps a decoded digest to its configured storage key.
func (b *BlobStore) ObjectKey(digest [32]byte) string { return b.key(digest) }

func (b *BlobStore) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		return fmt.Errorf("zstar s3: %w", fs.ErrClosed)
	case b.slots <- struct{}{}:
		select {
		case <-b.done:
			<-b.slots
			return fmt.Errorf("zstar s3: %w", fs.ErrClosed)
		case <-ctx.Done():
			<-b.slots
			return ctx.Err()
		default:
			return nil
		}
	}
}

// GetEncoded implements the entry-oriented resolver. The manifest's encoded
// size is a hint; enforce actual object length independently. Decoding and
// content-address verification belong to the archive reader.
func (b *BlobStore) GetEncoded(ctx context.Context, digest [32]byte, _, maxBytes uint64) (data []byte, err error) {
	if maxBytes == 0 {
		return nil, errors.New("zstar: encoded object budget must be positive")
	}
	if err := b.acquire(ctx); err != nil {
		return nil, objectError(err)
	}
	defer func() { <-b.slots }()
	// A single unranged GET replaces HEAD+GET. The object is content-addressed
	// and verified by the caller, so no version pinning is needed. The declared
	// length is checked before any body bytes are read.
	result, err := b.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(b.key(digest))})
	if err != nil {
		return nil, objectError(redacted(err))
	}
	if result == nil || result.Body == nil {
		return nil, errors.New("zstar s3: missing response body")
	}
	defer func() {
		if closeErr := result.Body.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("zstar s3: close response: %w", redacted(closeErr)))
			data = nil
		}
	}()
	if result.ContentLength == nil || *result.ContentLength <= 0 || uint64(*result.ContentLength) > maxBytes || uint64(*result.ContentLength) >= uint64(math.MaxInt) {
		return nil, &zstar.FormatError{Category: zstar.CategoryResourceLimit, Cause: errors.New("zstar s3: encoded object limit")}
	}
	data, err = readExact(result.Body, int(*result.ContentLength))
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

// Put uploads one object, skipping the write when the content-addressed key
// already exists (conditional If-None-Match), and returns its complete-object extent, ready for
// Header.Extents. Callers can select a subrange by adjusting Size and Offset.
func (b *BlobStore) Put(ctx context.Context, raw []byte) (extent zstar.Extent, err error) {
	if len(raw) == 0 {
		return zstar.Extent{}, errors.New("zstar s3: external raw size limit")
	}
	if err := b.acquire(ctx); err != nil {
		return zstar.Extent{}, objectError(err)
	}
	defer func() { <-b.slots }()
	digest := sha256.Sum256(raw)
	key := b.key(digest)
	if b.checkExists {
		meta, err := b.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)})
		if err == nil {
			if meta == nil || meta.ContentLength == nil || *meta.ContentLength <= 0 {
				return zstar.Extent{}, errors.New("zstar s3: invalid existing object size")
			}
			return zstar.Extent{Source: zstar.ObjectSource(digest), Size: uint64(len(raw)), EncodedSize: uint64(*meta.ContentLength)}, nil
		}
		if err = redacted(err); !errors.Is(err, ErrNotFound) {
			return zstar.Extent{}, err
		}
	}
	// An independent object cannot refer farther back than its own decoded size.
	// A power-of-two window bounds codec history without losing useful matches.
	window := max(zstd.MinWindowSize, 1<<bits.Len(uint(len(raw)-1)))
	encoder, err := b.takeEncoder(window)
	if err != nil {
		return zstar.Extent{}, err
	}
	encoder.output = encoder.codec.EncodeAll(raw, encoder.output[:0])
	defer func() {
		err = errors.Join(err, b.releaseEncoder(encoder))
		if err != nil {
			extent = zstar.Extent{}
		}
	}()
	encoded := encoder.output
	if err = ctx.Err(); err != nil {
		return zstar.Extent{}, err
	}
	_, err = b.client.PutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key), Body: bytes.NewReader(encoded), ContentLength: aws.Int64(int64(len(encoded))), ContentType: aws.String(zstdContentType), CacheControl: aws.String(immutableCacheControl), IfNoneMatch: aws.String("*")})
	// A precondition failure means the content-addressed object already exists.
	if api, ok := errors.AsType[smithy.APIError](err); err != nil && (!ok || api.ErrorCode() != "PreconditionFailed") {
		return zstar.Extent{}, objectError(redacted(err))
	}
	return zstar.Extent{Source: zstar.ObjectSource(digest), Size: uint64(len(raw)), EncodedSize: uint64(len(encoded))}, nil
}

func (b *BlobStore) takeEncoder(window int) (*objectEncoder, error) {
	b.mu.Lock()
	encoder := b.encoder
	b.encoder = nil
	b.mu.Unlock()
	if encoder != nil {
		if encoder.window == window {
			return encoder, nil
		}
		if err := encoder.codec.Close(); err != nil {
			return nil, err
		}
	}
	codec, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(window), zstd.WithEncoderCRC(true))
	if err != nil {
		return nil, err
	}
	return &objectEncoder{codec: codec, window: window}, nil
}

func (b *BlobStore) releaseEncoder(encoder *objectEncoder) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-b.done:
	default:
		if b.encoder == nil && encoder.window <= 1<<20 && cap(encoder.output) <= 2<<20 {
			b.encoder = encoder
			return nil
		}
	}
	return encoder.codec.Close()
}

// Close releases the idle codec and refuses new calls with fs.ErrClosed.
// Active calls finish and release their codecs. The SDK client stays open.
// It retains one idle codec with at most 1 MiB history and 2 MiB output, plus
// codec bookkeeping. Close is idempotent and concurrency safe.
func (b *BlobStore) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-b.done:
		return nil
	default:
		close(b.done)
	}
	encoder := b.encoder
	b.encoder = nil
	if encoder != nil {
		return encoder.codec.Close()
	}
	return nil
}

func objectError(err error) error {
	if errors.Is(err, ErrNotFound) {
		return &zstar.FormatError{Category: zstar.CategoryMissingExternalContent, Cause: err}
	}
	return err
}

var errObjectChanged = errors.New("zstar s3: object changed")
