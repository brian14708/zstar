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
	zstar "github.com/brian14708/zstar/sdks/go"
)

func metadataArchive(t *testing.T) ([]byte, zstar.Footer) {
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
	footer, err := zstar.ParseFooter(out.Bytes()[out.Len()-80:])
	if err != nil {
		t.Fatal(err)
	}
	return out.Bytes(), footer
}

type metadataUploadClient struct {
	uploadClient
	metadata map[string]string
}

type completeEOFReader struct{ *bytes.Reader }

func (r completeEOFReader) ReadAt(buffer []byte, offset int64) (int, error) {
	n, err := r.Reader.ReadAt(buffer, offset)
	if err == nil && n == len(buffer) {
		err = io.EOF
	}
	return n, err
}

func (c *metadataUploadClient) CreateMultipartUpload(ctx context.Context, in *awss3.CreateMultipartUploadInput, opts ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
	c.metadata = maps.Clone(in.Metadata)
	return c.uploadClient.CreateMultipartUpload(ctx, in, opts...)
}

func (c *metadataUploadClient) PutObject(ctx context.Context, in *awss3.PutObjectInput, opts ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	c.metadata = maps.Clone(in.Metadata)
	return c.uploadClient.PutObject(ctx, in, opts...)
}

func TestUploadCompletedArchivesAddsMetadataAndPreservesCallerOptions(t *testing.T) {
	data, footer := metadataArchive(t)
	expected, err := ArchiveMetadata(footer, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	expected["owner"] = "example"
	options := map[string]string{"owner": "example", "ZSTAR-VERSION": "bad"}
	client := &metadataUploadClient{}
	if err := Upload(context.Background(), client, "bucket", "key", bytes.NewReader(data), UploadOptions{Metadata: options}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.metadata, expected) {
		t.Fatalf("metadata: %v", client.metadata)
	}
	if options["ZSTAR-VERSION"] != "bad" || len(options) != 2 {
		t.Fatal("caller options mutated")
	}
	metadata, err := inputArchiveMetadata(completeEOFReader{bytes.NewReader(data)})
	if err != nil || metadata[ManifestPositionMetadataKey] != expected[ManifestPositionMetadataKey] {
		t.Fatalf("complete footer read with EOF: %v", err)
	}
	// An archive embedded after the caller's initial position is uploaded as
	// that suffix. Footer probing must leave that initial position unchanged.
	input := bytes.NewReader(append([]byte("prefix"), data...))
	if _, err := input.Seek(6, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	metadata, err = inputArchiveMetadata(input)
	if err != nil || metadata[ManifestPositionMetadataKey] != expected[ManifestPositionMetadataKey] {
		t.Fatal(err)
	}
	position, err := input.Seek(0, io.SeekCurrent)
	if err != nil || position != 6 {
		t.Fatalf("position=%d: %v", position, err)
	}
	// Streams have no discovered hints, but callers can supply known metadata.
	client = &metadataUploadClient{}
	if err := Upload(context.Background(), client, "bucket", "key", bytes.NewBuffer(data), UploadOptions{Metadata: expected}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.metadata, expected) {
		t.Fatal("stream annotations lost")
	}
	client = &metadataUploadClient{}
	if err := Upload(context.Background(), client, "bucket", "key", bytes.NewReader(nil), UploadOptions{Metadata: map[string]string{"owner": "example"}}); err != nil {
		t.Fatal(err)
	}
	if client.metadata["owner"] != "example" {
		t.Fatal("empty PUT lost metadata")
	}
	if _, err := ArchiveMetadata(footer, uint64(len(data))+1); err == nil {
		t.Fatal("invalid coverage accepted")
	}
}

type hintedClient struct {
	data        []byte
	metadata    map[string]string
	heads, gets int
}

func (c *hintedClient) HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	c.heads++
	return &awss3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(c.data))), ETag: aws.String("\"snapshot\""), Metadata: c.metadata}, nil
}

func (c *hintedClient) GetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	c.gets++
	if aws.ToString(in.IfMatch) != "\"snapshot\"" {
		return nil, errors.New("missing snapshot condition")
	}
	start, end, ok := strings.Cut(strings.TrimPrefix(aws.ToString(in.Range), "bytes="), "-")
	if !ok {
		return nil, errors.New("missing range")
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
		ContentRange: aws.String(fmt.Sprintf("bytes %d-%d/%d", offset, last, len(c.data))), ETag: aws.String("\"snapshot\""),
	}, nil
}

func TestManifestHintsSaveRequestAndFallBackSafely(t *testing.T) {
	data, footer := metadataArchive(t)
	good, err := ArchiveMetadata(footer, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		metadata map[string]string
		gets     int
	}{
		"present":              {good, 1},
		"position-only":        {map[string]string{ManifestPositionMetadataKey: good[ManifestPositionMetadataKey]}, 1},
		"absent":               {nil, 2},
		"malformed":            {map[string]string{ManifestPositionMetadataKey: "+1:10"}, 2},
		"overflow":             {map[string]string{ManifestPositionMetadataKey: "1:18446744073709551616"}, 2},
		"bounds":               {map[string]string{ManifestPositionMetadataKey: "0:1"}, 2},
		"unknown-version":      {map[string]string{VersionMetadataKey: "2", ManifestPositionMetadataKey: good[ManifestPositionMetadataKey]}, 2},
		"stale":                {map[string]string{ManifestPositionMetadataKey: fmt.Sprintf("%d:%d", footer.ManifestOffset-1, footer.ManifestEncodedSize+1)}, 3},
		"informational-digest": {map[string]string{ManifestPositionMetadataKey: good[ManifestPositionMetadataKey], ManifestSHA256MetadataKey: "bad"}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			client := &hintedClient{data: data, metadata: test.metadata}
			reader, err := zstar.Open(context.Background(), NewSource(client, "bucket", "key"))
			if err != nil {
				t.Fatal(err)
			}
			if client.heads != 1 || client.gets != test.gets {
				t.Fatalf("heads=%d gets=%d expected=%d", client.heads, client.gets, test.gets)
			}
			got, err := reader.Read(context.Background(), "x", 0, 5)
			if err != nil || string(got) != "hello" {
				t.Fatal(err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	client := &hintedClient{data: data, metadata: good}
	if _, err := zstar.OpenManifest(context.Background(), NewSource(client, "bucket", "key"), zstar.ManifestOptions{MaxEncodedBytes: 1}); zstar.ErrorCategory(err) != zstar.CategoryResourceLimit {
		t.Fatal(err)
	}
	if client.gets != 1 {
		t.Fatal("over-budget hint fetched")
	}
	corrupt := bytes.Clone(data)
	corrupt[len(corrupt)-1] ^= 1
	client = &hintedClient{data: corrupt, metadata: good}
	if _, err := zstar.Open(context.Background(), NewSource(client, "bucket", "key")); zstar.ErrorCategory(err) != zstar.CategoryIntegrityFailure {
		t.Fatal(err)
	}
}
