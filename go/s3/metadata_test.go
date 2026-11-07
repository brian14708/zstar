package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	zstar "github.com/brian14708/zstar/go"
)

func metadataArchive(t *testing.T) []byte {
	t.Helper()
	var out bytes.Buffer
	writer, err := zstar.NewWriter(&out, zstar.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(context.Background(), zstar.Header{Path: "x", Mode: 0o644, Size: 5}, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

type metadataUploadClient struct {
	uploadClient
	metadata                                   map[string]string
	contentType, cacheControl, contentEncoding string
}

func (c *metadataUploadClient) CreateMultipartUpload(ctx context.Context, in *awss3.CreateMultipartUploadInput, opts ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
	c.metadata = maps.Clone(in.Metadata)
	c.contentType, c.cacheControl, c.contentEncoding = aws.ToString(in.ContentType), aws.ToString(in.CacheControl), aws.ToString(in.ContentEncoding)
	return c.uploadClient.CreateMultipartUpload(ctx, in, opts...)
}

func (c *metadataUploadClient) PutObject(ctx context.Context, in *awss3.PutObjectInput, opts ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	c.metadata = maps.Clone(in.Metadata)
	c.contentType, c.cacheControl, c.contentEncoding = aws.ToString(in.ContentType), aws.ToString(in.CacheControl), aws.ToString(in.ContentEncoding)
	return c.uploadClient.PutObject(ctx, in, opts...)
}

func TestUploadPreservesCallerMetadata(t *testing.T) {
	data := metadataArchive(t)
	for _, metadata := range []map[string]string{nil, {"owner": "example"}} {
		client := &metadataUploadClient{}
		if err := Upload(context.Background(), client, "bucket", "archive.zstar", bytes.NewReader(data), UploadOptions{Metadata: metadata}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(client.metadata, metadata) {
			t.Fatalf("metadata changed: %v", client.metadata)
		}
		if client.contentType != zstdContentType || client.cacheControl != mutableCacheControl || client.contentEncoding != "" {
			t.Fatalf("content headers: %+v", client)
		}
	}
}

type hintedClient struct {
	data        []byte
	inputs      []*awss3.GetObjectInput
	metadata    map[string]string
	heads, gets int
}

func (c *hintedClient) HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	c.heads++
	return &awss3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(c.data))), ETag: aws.String("\"snapshot\""), Metadata: c.metadata}, nil
}

func (c *hintedClient) GetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	c.gets++
	c.inputs = append(c.inputs, in)
	if !strings.HasPrefix(aws.ToString(in.Range), "bytes=-") && aws.ToString(in.IfMatch) != "\"snapshot\"" {
		return nil, errors.New("missing snapshot condition")
	}
	start, end, ok := strings.Cut(strings.TrimPrefix(aws.ToString(in.Range), "bytes="), "-")
	if !ok {
		return nil, errors.New("missing range")
	}
	if start == "" {
		count, err := strconv.Atoi(end)
		if err != nil {
			return nil, err
		}
		start = strconv.Itoa(max(0, len(c.data)-count))
		end = strconv.Itoa(len(c.data) - 1)
	}
	offset, err := strconv.Atoi(start)
	if err != nil {
		return nil, err
	}
	last, err := strconv.Atoi(end)
	if err != nil {
		return nil, err
	}
	raw := c.data[offset : last+1]
	return &awss3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(raw)), ContentLength: aws.Int64(int64(len(raw))),
		ContentRange: aws.String(fmt.Sprintf("bytes %d-%d/%d", offset, last, len(c.data))), ETag: aws.String("\"snapshot\""), Metadata: c.metadata,
	}, nil
}

func TestSuffixPrefetchWithoutObjectMetadata(t *testing.T) {
	data := metadataArchive(t)
	client := &hintedClient{data: data}
	reader, err := zstar.Open(context.Background(), NewSource(client, "bucket", "key"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.Read(context.Background(), "x", 0, 5)
	if err != nil || string(got) != "hello" {
		t.Fatalf("read: %q %v", got, err)
	}
	if client.heads != 0 || client.gets != 1 || aws.ToString(client.inputs[0].Range) != "bytes=-131072" {
		t.Fatalf("heads=%d gets=%d", client.heads, client.gets)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(data)
	corrupt[len(corrupt)-zstar.FooterSize-1] ^= 1
	client = &hintedClient{data: corrupt}
	if _, err := zstar.Open(context.Background(), NewSource(client, "bucket", "key")); zstar.ErrorCategory(err) != zstar.CategoryIntegrityFailure {
		t.Fatal(err)
	}
}

func TestUploadExplicitContentHeadersAndImmutableDefaults(t *testing.T) {
	for _, test := range []struct {
		key, kind, cache string
		options          UploadOptions
	}{
		{"report.v1.ZSTAR", zstdContentType, immutableCacheControl, UploadOptions{Immutable: true}},
		{"catalog.cbor", "application/cbor", mutableCacheControl, UploadOptions{}},
		{"unknown", "application/custom", "no-store", UploadOptions{ContentType: "application/custom", CacheControl: "no-store"}},
	} {
		client := &metadataUploadClient{}
		if err := Upload(context.Background(), client, "bucket", test.key, bytes.NewReader(nil), test.options); err != nil {
			t.Fatal(err)
		}
		if client.contentType != test.kind || client.cacheControl != test.cache || client.contentEncoding != "" {
			t.Fatalf("%s: %+v", test.key, client)
		}
	}
}
