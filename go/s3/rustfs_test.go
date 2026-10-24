//go:build integration

package s3

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	zstar "github.com/brian14708/zstar/go"
)

func TestRustFSNativeArchiveObjectsAndPresignedRanges(t *testing.T) {
	endpoint := os.Getenv("ZSTAR_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires local RustFS")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "http" || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" && parsed.Hostname() != "::1") {
		t.Fatal("requires loopback RustFS endpoint")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := awss3.New(awss3.Options{BaseEndpoint: aws.String(endpoint), UsePathStyle: true, Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("zstar-test", "zstar-test-secret", "")})
	bucket := "zstar-go-native-" + time.Now().Format("150405000000000")
	if _, err = client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(redacted(err))
	}
	defer func() {
		if _, err := client.DeleteBucket(context.Background(), &awss3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Error(err)
		}
	}()
	t.Run("packed multipart and publication", func(t *testing.T) { testPackedRustFS(ctx, t, client, bucket) })
	t.Run("presigned GET and PUT", func(t *testing.T) {
		testPresignedGETAndPUT(ctx, t, client, bucket)
	})
	blobs, err := NewBlobStore(client, bucket, BlobStoreOptions{Prefix: "objects", Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	object := []byte("prefix:remote contents:suffix")
	uploaded, err := blobs.Put(ctx, object)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := zstar.ParseContentAddress(uploaded.Source.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := client.DeleteObject(context.Background(), &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(blobs.ObjectKey(digest))}); err != nil {
			t.Error(err)
		}
	}()
	assertObjectHeaders(ctx, t, client, bucket, blobs.ObjectKey(digest), zstdContentType, immutableCacheControl)
	encoded, err := blobs.GetEncoded(ctx, digest, 1, 17<<20)
	if err != nil || len(encoded) == 0 || uint64(len(encoded)) != uploaded.EncodedSize {
		t.Fatalf("object read: %v", err)
	}
	var archiveBytes bytes.Buffer
	writer, err := zstar.NewWriter(&archiveBytes, zstar.WriterOptions{FrameSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Add(ctx, zstar.Header{Path: "inline", Mode: 0o644, Size: 5}, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatal(err)
	}
	selected := uploaded
	selected.Offset = 7
	selected.Size = 15
	if err = writer.Add(ctx, zstar.Header{Path: "remote", Mode: 0o644, Extents: []zstar.Extent{selected}}, nil); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	key := "archive.zstar"
	defer func() {
		if _, err := client.DeleteObject(context.Background(), &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
			t.Error(err)
		}
	}()
	if err = Upload(ctx, client, bucket, key, bytes.NewReader(archiveBytes.Bytes()), UploadOptions{}); err != nil {
		t.Fatal(redacted(err))
	}
	source := NewSource(client, bucket, key)
	reader, err := zstar.OpenWithOptions(ctx, source, zstar.ReaderOptions{Resolver: blobs})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := reader.Verify(ctx, true); err != nil {
		t.Fatal(err)
	}
	if got, err := reader.Read(ctx, "inline", 1, 3); err != nil || string(got) != "ell" {
		t.Fatalf("inline: %q %v", got, err)
	}
	if got, err := reader.Read(ctx, "remote", 0, 15); err != nil || string(got) != "remote contents" {
		t.Fatalf("external: %q %v", got, err)
	}
	grouped, err := reader.ReadMany(ctx, []zstar.ReadRequest{
		{Path: "inline", Offset: 0, Length: 5},
		{Path: "remote", Offset: 0, Length: 15},
		{Path: "remote", Offset: 7, Length: 8},
		{Path: "inline", Offset: 1, Length: 3},
	})
	if err != nil || len(grouped) != 4 || string(grouped[0]) != "hello" || string(grouped[1]) != "remote contents" || string(grouped[2]) != "contents" || string(grouped[3]) != "ell" {
		t.Fatalf("native mixed batch: %q %v", grouped, err)
	}
	signed, err := awss3.NewPresignClient(client).PresignGetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(redacted(err))
	}
	remote, err := zstar.OpenWithOptions(ctx, &zstar.HTTPRangeReader{URL: signed.URL, Headers: signed.SignedHeader}, zstar.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := remote.Close(); err != nil {
			t.Error(err)
		}
	}()
	if got, err := remote.Read(ctx, "inline", 0, 5); err != nil || string(got) != "hello" {
		t.Fatalf("presigned: %q %v", got, err)
	}
	grouped, err = remote.ReadMany(ctx, []zstar.ReadRequest{
		{Path: "inline", Offset: 1, Length: 3},
		{Path: "inline", Offset: 0, Length: 5},
	})
	if err != nil || len(grouped) != 2 || string(grouped[0]) != "ell" || string(grouped[1]) != "hello" {
		t.Fatalf("presigned batch: %q %v", grouped, err)
	}
	multipartKey := "multipart.bin"
	defer func() {
		if _, err := client.DeleteObject(context.Background(), &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(multipartKey)}); err != nil {
			t.Error(err)
		}
	}()
	payload := bytes.Repeat([]byte("z"), 11<<20)
	if err = Upload(ctx, client, bucket, multipartKey, bytes.NewReader(payload), UploadOptions{PartSize: 5 << 20, Concurrency: 2}); err != nil {
		t.Fatal(err)
	}
	multipart := NewSource(client, bucket, multipartKey)
	if size, err := multipart.Size(ctx); err != nil || size != int64(len(payload)) {
		t.Fatalf("multipart size %d %v", size, err)
	}
	if got, err := multipart.ReadRange(ctx, (5<<20)-4, 8); err != nil || string(got) != "zzzzzzzz" {
		t.Fatalf("multipart %q %v", got, err)
	}
	emptyKey := "empty.bin"
	defer func() {
		if _, err := client.DeleteObject(context.Background(), &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(emptyKey)}); err != nil {
			t.Error(err)
		}
	}()
	if err = Upload(ctx, client, bucket, emptyKey, bytes.NewReader(nil), UploadOptions{PartSize: 5 << 20, Concurrency: 2}); err != nil {
		t.Fatal(err)
	}
	if size, err := NewSource(client, bucket, emptyKey).Size(ctx); err != nil || size != 0 {
		t.Fatalf("empty upload %d %v", size, err)
	}
	// The source pins ETag even if a replacement has exactly the same length.
	changed := bytes.Clone(archiveBytes.Bytes())
	changed[0] ^= 1
	if _, err = client.PutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(changed)}); err != nil {
		t.Fatal(redacted(err))
	}
	if _, err = source.ReadRange(ctx, 0, 1); err == nil {
		t.Fatal("replaced archive accepted")
	}
}

func testPackedRustFS(ctx context.Context, t *testing.T, client *awss3.Client, bucket string) {
	t.Helper()
	store := NewPackStore(client, bucket, "packed", UploadOptions{PartSize: 5 << 20, Concurrency: 2})
	writer, err := zstar.NewPackWriter(store, nil, zstar.PackWriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			t.Error(err)
		}
	}()
	extents := make([]zstar.Extent, 0, 3)
	expected := make([]byte, 0, 12<<20)
	state := uint64(123)
	for range 3 {
		raw := make([]byte, 0, 4<<20)
		for range (4 << 20) / 8 {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			for i := range 8 {
				raw = append(raw, byte(state>>(i*8)))
			}
		}
		e, err := writer.Put(ctx, raw)
		if err != nil {
			t.Fatal(err)
		}
		extents = append(extents, e)
		expected = append(expected, raw...)
	}
	catalog, err := writer.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := catalog.Packs()[0].Snapshot
	assertObjectHeaders(ctx, t, client, bucket, "packed/"+snapshot.Key, zstdContentType, immutableCacheControl)
	if snapshot.Length <= 10<<20 {
		t.Fatal("multipart path not exercised")
	}
	defer func() {
		listed, err := client.ListObjectsV2(context.Background(), &awss3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("packed/")})
		if err != nil {
			t.Error(err)
			return
		}
		for _, object := range listed.Contents {
			if _, err := client.DeleteObject(context.Background(), &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: object.Key}); err != nil {
				t.Error(err)
			}
		}
	}()
	resolver, err := zstar.NewPackedObjectResolver(store, catalog, zstar.PackedResolverOptions{MaxRangeBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	var thin bytes.Buffer
	archiveWriter, err := zstar.NewWriter(&thin, zstar.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = archiveWriter.Add(ctx, zstar.Header{Path: "file", Mode: 0o644, Extents: extents}, nil); err != nil {
		t.Fatal(err)
	}
	if err = archiveWriter.Close(); err != nil {
		t.Fatal(err)
	}
	archive, err := zstar.OpenWithOptions(ctx, zstar.ReaderAtSource{Reader: bytes.NewReader(thin.Bytes()), Length: int64(thin.Len())}, zstar.ReaderOptions{Resolver: resolver, DisableContentCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := archive.Close(); err != nil {
			t.Error(err)
		}
	}()
	out, err := archive.Read(ctx, "file", 0, uint64(len(expected)))
	if err != nil || !bytes.Equal(out, expected) {
		t.Fatal("packed reconstruction", err)
	}
	descriptor, g, err := zstar.PublishPackedGeneration(ctx, store, "run", bytes.NewReader(thin.Bytes()), catalog, map[string]any{"implementation": "Go"})
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range []struct{ key, kind string }{{descriptor.Key, "application/cbor"}, {g.Archive.Key, zstdContentType}, {g.Catalog.Key, "application/cbor"}} {
		assertObjectHeaders(ctx, t, client, bucket, "packed/"+artifact.key, artifact.kind, immutableCacheControl)
	}
	raw, err := store.ReadPackedRange(ctx, descriptor, 0, descriptor.Length)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := zstar.DecodePackedGeneration(raw)
	if err != nil || decoded.Archive != g.Archive {
		t.Fatal("generation", err)
	}
	raw, err = store.ReadPackedRange(ctx, snapshot, 0, snapshot.Length)
	if err != nil {
		t.Fatal(err)
	}
	alternative := NewPackStore(client, bucket, "packed", UploadOptions{PartSize: 8 << 20, Concurrency: 2})
	reused, err := alternative.PutPack(ctx, snapshot.Key, bytes.NewReader(raw), snapshot.Length)
	if err != nil || reused != snapshot {
		t.Fatal("immutable reuse", err)
	}
}

func assertObjectHeaders(ctx context.Context, t *testing.T, client *awss3.Client, bucket, key, kind, cache string) {
	t.Helper()
	head, err := client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(head.ContentType) != kind || aws.ToString(head.CacheControl) != cache || aws.ToString(head.ContentEncoding) != "" {
		t.Fatalf("%s: content-type=%q cache-control=%q encoding=%q", key, aws.ToString(head.ContentType), aws.ToString(head.CacheControl), aws.ToString(head.ContentEncoding))
	}
}
