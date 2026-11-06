package zstar

import (
	"context"
	"crypto/sha256"
	"io"
	"testing"
)

type pluggableObjectWriter struct{}

func (pluggableObjectWriter) Put(_ context.Context, raw []byte) (Extent, error) {
	digest := sha256.Sum256(raw)
	return Extent{Size: uint64(len(raw)), Source: ObjectSource(digest), EncodedSize: 1}, nil
}

func TestPluggableStreamOptionsAndContentBudgets(t *testing.T) {
	ctx := context.Background()
	reader, err := OpenWithOptions(ctx, memoryRange(buildTestArchive(t, 1024)), ReaderOptions{Manifest: ManifestReaderOptions{ConcurrentChunks: 1, MaxReadBytes: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, reader)
	stream, err := reader.OpenStream(ctx, "nested/data", StreamOptions{DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, stream)
	data := make([]byte, 16)
	if _, err := io.ReadFull(stream, data); err != nil {
		t.Fatal(err)
	}
	if stream.chunk != nil {
		t.Fatal("disabled cache retained content")
	}
	if _, err := reader.Read(ctx, "nested/data", 0, 2); ErrorCategory(err) != CategoryResourceLimit {
		t.Fatal(err)
	}
}

func TestExternalizeCallerSelectedChunks(t *testing.T) {
	chunks := func(yield func([]byte, error) bool) {
		if yield([]byte("abc"), nil) {
			yield([]byte("def"), nil)
		}
	}
	extents, err := ExternalizeChunks(context.Background(), chunks, 6, pluggableObjectWriter{})
	if err != nil || len(extents) != 2 {
		t.Fatal(extents, err)
	}
	if _, err := ExternalizeChunks(context.Background(), chunks, 5, pluggableObjectWriter{}); err == nil {
		t.Fatal("accepted extra bytes")
	}
	if _, err := ExternalizeChunks(context.Background(), chunks, 7, pluggableObjectWriter{}); err == nil {
		t.Fatal("accepted short input")
	}
}
