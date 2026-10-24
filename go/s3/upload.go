package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// MultipartClient provides the S3 operations needed for bounded multipart upload.
type MultipartClient interface {
	CreateMultipartUpload(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error)
	PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
}

// UploadOptions bounds part-buffer size and concurrent multipart requests.
type UploadOptions struct {
	// Immutable refuses replacing a completed object at this key.
	Immutable bool
	// ContentType and CacheControl override the inferred media type and cache policy.
	ContentType  string
	CacheControl string
	// Metadata is copied into the upload. Completed ReaderAt/Seeker archives
	// automatically replace the three reserved zstar keys with footer values.
	Metadata    map[string]string
	PartSize    int // zero selects 8 MiB; [5 MiB,64 MiB]
	Concurrency int // zero selects four; [1,32]
}

// Upload streams an archive through bounded concurrent native S3 multipart
// parts. Failed or cancelled uploads attempt an independent timed abort. Input
// reads are synchronous: callers must unblock a stalled custom io.Reader when
// cancelling, for example by closing their pipe. The input remains caller-owned.
func Upload(ctx context.Context, client MultipartClient, bucket, key string, input io.Reader, options UploadOptions) (err error) {
	if options.PartSize == 0 {
		options.PartSize = 8 << 20
	}
	if options.Concurrency == 0 {
		options.Concurrency = 4
	}
	if input == nil || client == nil || options.PartSize < 5<<20 || options.PartSize > 64<<20 || options.Concurrency < 1 || options.Concurrency > 32 {
		return errors.New("zstar s3: invalid upload options")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	metadata := maps.Clone(options.Metadata)
	archiveMetadata, err := inputArchiveMetadata(input)
	if err != nil {
		return err
	}
	if archiveMetadata != nil {
		if metadata == nil {
			metadata = make(map[string]string)
		}
		for key := range metadata {
			for reserved := range archiveMetadata {
				if strings.EqualFold(key, reserved) {
					delete(metadata, key)
					break
				}
			}
		}
		maps.Copy(metadata, archiveMetadata)
	}
	contentType := options.ContentType
	if contentType == "" {
		contentType = artifactContentType(key)
		if archiveMetadata != nil {
			contentType = zstdContentType
		}
	}
	cache := options.CacheControl
	if cache == "" {
		cache = cacheControl(options.Immutable)
	}
	created, err := client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), Metadata: metadata, ContentType: aws.String(contentType), CacheControl: aws.String(cache)})
	if err != nil {
		return redacted(err)
	}
	if created == nil || aws.ToString(created.UploadId) == "" {
		return errors.New("zstar s3: missing multipart upload id")
	}
	id := created.UploadId
	completed := false
	// emptyStored marks a successful zero-length PutObject. The leftover empty
	// multipart session is still aborted, but an abort failure must not turn
	// that success into an error.
	emptyStored := false
	defer func() {
		if !completed {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_, abortErr := client.AbortMultipartUpload(cleanup, &awss3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id})
			if abortErr != nil && !emptyStored {
				err = errors.Join(err, errors.New("zstar s3: multipart abort failed"))
			}
		}
	}()
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	slots := make(chan struct{}, options.Concurrency)
	var group sync.WaitGroup
	var mu sync.Mutex
	var parts []types.CompletedPart
	var firstError error
	fail := func(errorValue error) {
		mu.Lock()
		if firstError == nil {
			firstError = errorValue
			cancel()
		}
		mu.Unlock()
	}
	var number int32
readLoop:
	for {
		select {
		case <-work.Done():
			break readLoop
		case slots <- struct{}{}:
		}
		raw := make([]byte, options.PartSize)
		n, readErr := io.ReadFull(input, raw)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			<-slots
			fail(readErr)
			break
		}
		if n == 0 {
			<-slots
			break
		}
		number++
		if number > 10000 {
			<-slots
			fail(errors.New("zstar s3: multipart part count limit"))
			break
		}
		partNumber := number
		raw = raw[:n]
		group.Go(func() {
			defer func() { <-slots }()
			result, uploadErr := client.UploadPart(work, &awss3.UploadPartInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id, PartNumber: aws.Int32(partNumber), Body: bytes.NewReader(raw), ContentLength: aws.Int64(int64(len(raw)))})
			if uploadErr != nil {
				fail(redacted(uploadErr))
				return
			}
			if result == nil || aws.ToString(result.ETag) == "" {
				fail(errors.New("zstar s3: missing part ETag"))
				return
			}
			mu.Lock()
			parts = append(parts, types.CompletedPart{ETag: result.ETag, PartNumber: aws.Int32(partNumber)})
			mu.Unlock()
		})
		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}
	group.Wait()
	if firstError != nil {
		return firstError
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if len(parts) == 0 {
		// S3 cannot complete a zero-part upload. The defer aborts its empty session.
		_, err = client.PutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(nil), ContentLength: aws.Int64(0), Metadata: metadata, ContentType: aws.String(contentType), CacheControl: aws.String(cache), IfNoneMatch: immutableCondition(options.Immutable)})
		if err != nil {
			return redacted(err)
		}
		emptyStored = true
		return nil
	}
	sort.Slice(parts, func(i, j int) bool { return *parts[i].PartNumber < *parts[j].PartNumber })
	_, err = client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}, IfNoneMatch: immutableCondition(options.Immutable)})
	if err != nil {
		if options.Immutable {
			return immutableError(err)
		}
		return redacted(err)
	}
	completed = true
	return nil
}

func immutableCondition(enabled bool) *string {
	if enabled {
		return aws.String("*")
	}
	return nil
}
