package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type abortFailClient struct{ uploadClient }

func (c *abortFailClient) AbortMultipartUpload(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
	return nil, errors.New("abort denied")
}

func TestEmptyUploadSucceedsWhenAbortFails(t *testing.T) {
	client := &abortFailClient{}
	if err := Upload(context.Background(), client, "bucket", "key", bytes.NewReader(nil), UploadOptions{}); err != nil {
		t.Fatalf("empty upload masked by abort failure: %v", err)
	}
	if client.puts != 1 {
		t.Fatalf("puts=%d", client.puts)
	}
	// A real failure still reports the abort failure.
	client = &abortFailClient{uploadClient{fail: true}}
	if err := Upload(context.Background(), client, "bucket", "key", bytes.NewReader(make([]byte, 6<<20)), UploadOptions{}); err == nil {
		t.Fatal("expected failure")
	}
}

type uploadClient struct {
	mu                                    sync.Mutex
	active, peak, aborts, completes, puts int
	fail                                  bool
	cancel                                context.CancelFunc
	parts                                 map[int32]int
}

func (c *uploadClient) CreateMultipartUpload(ctx context.Context, in *awss3.CreateMultipartUploadInput, opts ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
	return &awss3.CreateMultipartUploadOutput{UploadId: aws.String("id")}, nil
}

func (c *uploadClient) UploadPart(ctx context.Context, in *awss3.UploadPartInput, opts ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
	c.mu.Lock()
	c.active++
	c.peak = max(c.peak, c.active)
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.active--; c.mu.Unlock() }()
	if c.cancel != nil {
		c.cancel()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Millisecond):
	}
	n, err := io.Copy(io.Discard, in.Body)
	if err != nil {
		return nil, err
	}
	if c.fail {
		return nil, errors.New("transport error with signed URL ?secret=credential")
	}
	c.mu.Lock()
	if c.parts == nil {
		c.parts = make(map[int32]int)
	}
	c.parts[*in.PartNumber] = int(n)
	c.mu.Unlock()
	return &awss3.UploadPartOutput{ETag: aws.String("etag")}, nil
}

func (c *uploadClient) CompleteMultipartUpload(ctx context.Context, in *awss3.CompleteMultipartUploadInput, opts ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
	c.completes++
	for i, part := range in.MultipartUpload.Parts {
		if *part.PartNumber != int32(i+1) {
			return nil, errors.New("parts out of order")
		}
	}
	return &awss3.CompleteMultipartUploadOutput{}, nil
}

func (c *uploadClient) AbortMultipartUpload(ctx context.Context, in *awss3.AbortMultipartUploadInput, opts ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.aborts++
	return &awss3.AbortMultipartUploadOutput{}, nil
}

func (c *uploadClient) PutObject(ctx context.Context, in *awss3.PutObjectInput, opts ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	c.puts++
	return &awss3.PutObjectOutput{}, nil
}

func TestMultipartBoundsOrderingEmptyFailureAndCancellation(t *testing.T) {
	options := UploadOptions{PartSize: 5 << 20, Concurrency: 2}
	client := &uploadClient{}
	if err := Upload(context.Background(), client, "bucket", "key", bytes.NewReader(make([]byte, 11<<20)), options); err != nil {
		t.Fatal(err)
	}
	if client.completes != 1 || client.aborts != 0 || client.peak > 2 || client.active != 0 || len(client.parts) != 3 {
		t.Fatalf("unexpected upload: %+v", client)
	}
	if client.parts[1] != 5<<20 || client.parts[2] != 5<<20 || client.parts[3] != 1<<20 {
		t.Fatal("part sizes")
	}
	client = &uploadClient{}
	if err := Upload(context.Background(), client, "bucket", "key", bytes.NewReader(nil), options); err != nil {
		t.Fatal(err)
	}
	if client.completes != 0 || client.aborts != 1 || client.puts != 1 {
		t.Fatalf("empty upload: %+v", client)
	}
	client = &uploadClient{fail: true}
	if err := Upload(context.Background(), client, "bucket", "key", bytes.NewReader(make([]byte, 11<<20)), options); err == nil || err.Error() != "zstar s3: storage request failed" {
		t.Fatalf("unredacted or missing error: %v", err)
	}
	if client.aborts != 1 || client.completes != 0 || client.active != 0 {
		t.Fatalf("failed cleanup: %+v", client)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client = &uploadClient{cancel: cancel}
	if err := Upload(ctx, client, "bucket", "key", bytes.NewReader(make([]byte, 11<<20)), options); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if client.aborts != 1 || client.completes != 0 || client.active != 0 {
		t.Fatalf("cancel cleanup: %+v", client)
	}
}
