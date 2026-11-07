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
	etag, version *string
}

func (c metadataClient) HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	return &awss3.HeadObjectOutput{ContentLength: aws.Int64(3), ETag: c.etag, VersionId: c.version}, nil
}

func (c metadataClient) GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	return &awss3.GetObjectOutput{
		Body:          io.NopCloser(strings.NewReader("abc")),
		ContentLength: aws.Int64(3), ContentRange: aws.String("bytes 0-2/3"),
		ETag: c.etag, VersionId: c.version,
	}, nil
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

func TestSourcePrefetchIsBoundedAndReusesPartialRanges(t *testing.T) {
	client := &hintedClient{data: []byte("0123456789")}
	source := NewSource(client, "bucket", "key").WithMaxRangeBytes(4)
	if size, err := source.Size(context.Background()); err != nil || size != 10 {
		t.Fatalf("size: %d %v", size, err)
	}
	for _, request := range []struct {
		offset, length int64
		want           string
	}{
		{8, 2, "89"}, {4, 4, "4567"}, {6, 4, "6789"},
	} {
		got, err := source.ReadRange(context.Background(), request.offset, request.length)
		if err != nil || string(got) != request.want {
			t.Fatalf("read: %q %v", got, err)
		}
		got[0] = 'x' // Caller ownership must not mutate the source cache.
	}
	if client.heads != 0 || client.gets != 2 || aws.ToString(client.inputs[0].Range) != "bytes=-4" ||
		aws.ToString(client.inputs[1].Range) != "bytes=4-5" || aws.ToString(client.inputs[1].IfMatch) != "\"snapshot\"" {
		t.Fatalf("unexpected requests: heads=%d inputs=%+v", client.heads, client.inputs)
	}
	if _, err := source.ReadRange(context.Background(), 0, 5); err == nil || client.gets != 2 {
		t.Fatalf("range budget ignored: %v", err)
	}
}

type suffixResponseClient struct {
	readOnlyClient
	response *awss3.GetObjectOutput
}

func (c suffixResponseClient) GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	return c.response, nil
}

func TestSourceRejectsMalformedSuffixResponses(t *testing.T) {
	for _, test := range []struct {
		contentRange string
		length       int64
		body         string
	}{
		{"", 3, "abc"},
		{"bytes 1-2/3", 2, "bc"},
		{"bytes 0-1/3", 2, "ab"},
		{"bytes 0-2/3 trailing", 3, "abc"},
		{"bytes 0-2/3", 4, "abcd"},
		{"bytes 0-2/3", 3, "ab"},
		{"bytes 0-2/3", 3, "abcd"},
	} {
		client := suffixResponseClient{response: &awss3.GetObjectOutput{
			ContentRange: new(test.contentRange), ContentLength: new(test.length),
			Body: io.NopCloser(strings.NewReader(test.body)), ETag: new("snapshot"),
		}}
		if _, err := NewSource(client, "bucket", "key").Size(context.Background()); err == nil {
			t.Fatalf("accepted malformed suffix: %+v", test)
		}
	}
}

type changedSourceClient struct{ hintedClient }

func (c *changedSourceClient) GetObject(ctx context.Context, in *awss3.GetObjectInput, options ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	result, err := c.hintedClient.GetObject(ctx, in, options...)
	if err == nil && !strings.HasPrefix(aws.ToString(in.Range), "bytes=-") {
		result.ETag = new("changed")
	}
	return result, err
}

func TestSourceUncachedReadsRejectChangedIdentity(t *testing.T) {
	client := &changedSourceClient{hintedClient{data: []byte("0123456789")}}
	source := NewSource(client, "bucket", "key").WithMaxRangeBytes(4)
	if _, err := source.Size(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ReadRange(context.Background(), 0, 2); !errors.Is(err, errObjectChanged) {
		t.Fatalf("changed object accepted: %v", err)
	}
}
