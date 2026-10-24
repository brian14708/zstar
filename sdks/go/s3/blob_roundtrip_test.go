package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type countingBlobClient struct {
	heads, gets int
	body        []byte
	putErr      error
	ifNoneMatch *string
}

func (c *countingBlobClient) HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	c.heads++
	return nil, errors.New("unexpected HEAD")
}

func (c *countingBlobClient) GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	c.gets++
	return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(c.body)), ContentLength: aws.Int64(int64(len(c.body)))}, nil
}

func (c *countingBlobClient) PutObject(_ context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	c.ifNoneMatch = in.IfNoneMatch
	return &awss3.PutObjectOutput{}, c.putErr
}

func TestBlobGetEncodedSingleRoundTripAndLimit(t *testing.T) {
	c := &countingBlobClient{body: []byte("hello")}
	store, err := NewBlobStore(c, "b", BlobStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.GetEncoded(context.Background(), [32]byte{}, 0, 100)
	if err != nil || string(got) != "hello" || c.heads != 0 || c.gets != 1 {
		t.Fatalf("got=%q err=%v heads=%d gets=%d", got, err, c.heads, c.gets)
	}
	if _, err = store.GetEncoded(context.Background(), [32]byte{}, 0, 3); err == nil {
		t.Fatal("limit not enforced")
	}
}

func TestBlobPutDedupesExistingObject(t *testing.T) {
	c := &countingBlobClient{putErr: &smithy.GenericAPIError{Code: "PreconditionFailed"}}
	store, err := NewBlobStore(c, "b", BlobStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Put(context.Background(), []byte("x")); err != nil {
		t.Fatalf("existing object should succeed: %v", err)
	}
	if aws.ToString(c.ifNoneMatch) != "*" {
		t.Fatal("missing If-None-Match")
	}
	c.putErr = &smithy.GenericAPIError{Code: "NoSuchBucket"}
	_, err = store.Put(context.Background(), []byte("x"))
	if err == nil || !errors.Is(err, ErrNotFound) {
		t.Fatalf("put error not routed: %v", err)
	}
}
