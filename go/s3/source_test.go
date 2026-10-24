package s3

import (
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// This adapter deliberately has no PutObject method.
type readOnlyClient struct{}

var _ SourceClient = readOnlyClient{}

func (readOnlyClient) HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	return &awss3.HeadObjectOutput{ContentLength: aws.Int64(3), ETag: aws.String("version")}, nil
}

func (readOnlyClient) GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	return &awss3.GetObjectOutput{
		Body:          io.NopCloser(strings.NewReader("abc")),
		ContentLength: aws.Int64(3),
		ContentRange:  aws.String("bytes 0-2/3"),
		ETag:          aws.String("version"),
	}, nil
}

func TestSourceAcceptsReadOnlyClient(t *testing.T) {
	source := NewSource(readOnlyClient{}, "bucket", "archive")
	data, err := source.ReadRange(context.Background(), 0, 3)
	if err != nil || string(data) != "abc" {
		t.Fatalf("read-only source: %q, %v", data, err)
	}
}

type metadataClient struct {
	readOnlyClient
	etag, version *string
}

func (c metadataClient) HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	return &awss3.HeadObjectOutput{ContentLength: aws.Int64(3), ETag: c.etag, VersionId: c.version}, nil
}

func TestSourceRejectsMissingImmutableIdentity(t *testing.T) {
	for _, client := range []metadataClient{
		{},
		{etag: aws.String("")},
		{version: aws.String("null")},
		{etag: aws.String(""), version: aws.String("")},
	} {
		if _, err := NewSource(client, "bucket", "key").Size(context.Background()); err == nil {
			t.Fatalf("accepted unusable identity: %+v", client)
		}
	}
	for _, client := range []metadataClient{
		{etag: aws.String("etag"), version: aws.String("null")},
		{version: aws.String("version")},
	} {
		if _, err := NewSource(client, "bucket", "key").Size(context.Background()); err != nil {
			t.Fatalf("rejected immutable identity: %v", err)
		}
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestExactBodyBoundsCancellationAndRedaction(t *testing.T) {
	for _, body := range []string{"", "ab", "abcd"} {
		if _, err := readExact(strings.NewReader(body), 3); err == nil {
			t.Fatalf("accepted body %q", body)
		}
	}
	if got, err := readExact(strings.NewReader("abc"), 3); err != nil || string(got) != "abc" {
		t.Fatalf("exact response: %q, %v", got, err)
	}
	for _, count := range []int{-1, math.MaxInt} {
		if _, err := readExact(strings.NewReader(""), count); err == nil {
			t.Fatalf("accepted allocation count %d", count)
		}
	}
	for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
		_, err := readExact(errorReader{err: errors.Join(want, errors.New("secret URL"))}, 3)
		if !errors.Is(err, want) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("lost cancellation or leaked backend error: %v", err)
		}
	}
	_, err := readExact(errorReader{err: errors.New("secret URL")}, 3)
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("missing or unredacted error: %v", err)
	}
}
