package zstar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"testing"
)

type pluggableObjectWriter struct{}

func (pluggableObjectWriter) Put(_ context.Context, raw []byte) (Extent, error) {
	digest := sha256.Sum256(raw)
	return Extent{Size: uint64(len(raw)), Source: ObjectSource(digest), EncodedSize: 1}, nil
}

func TestStreamPrivateCacheAndContentBudgets(t *testing.T) {
	ctx := context.Background()
	reader, err := OpenWithOptions(ctx, memoryRange(buildTestArchive(t, 1024)), ReaderOptions{Manifest: ManifestReaderOptions{ConcurrentChunks: 1, MaxReadBytes: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, reader)
	stream, err := reader.OpenStream(ctx, "nested/data")
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, stream)
	data := make([]byte, 16)
	if _, err := io.ReadFull(stream, data); err != nil {
		t.Fatal(err)
	}
	if stream.file.cache.chunk == nil {
		t.Fatal("small decoded source was not retained")
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

func TestStreamCacheReplacementAndLifetimeChecks(t *testing.T) {
	ctx := context.Background()
	source := &manifestCountedRange{memoryRange: buildTestArchive(t, 1024)}
	reader, err := Open(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, reader)
	stream, err := reader.OpenStream(ctx, "nested/data")
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, stream)
	source.calls = 0
	for _, offset := range []int64{0, 1, 5999, 0} {
		if _, err := stream.Seek(offset, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		var data [1]byte
		if _, err := stream.Read(data[:]); err != nil {
			t.Fatal(err)
		}
		if data[0] != "abcd"[offset%4] {
			t.Fatalf("wrong byte at %d", offset)
		}
	}
	if source.calls != 3 {
		t.Fatalf("cache reuse/replacement: %d fetches", source.calls)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	var data [1]byte
	if _, err := stream.ReadContext(canceled, data[:]); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached read ignored cancellation: %v", err)
	}
	testClose(t, reader)
	if _, err := stream.Read(data[:]); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("cached read ignored reader closure: %v", err)
	}
}

func TestStreamSkipsOversizedDecodedSources(t *testing.T) {
	ctx := context.Background()
	var output bytes.Buffer
	writer := mustTestWriter(t, &output, WriterOptions{FrameSize: 4 << 20})
	payload := bytes.Repeat([]byte{7}, 3<<20)
	if err := writer.Add(ctx, Header{Path: "file", Mode: 0o644, Size: uint64(len(payload))}, bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	testClose(t, writer)
	source := &manifestCountedRange{memoryRange: output.Bytes()}
	reader, err := Open(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, reader)
	stream, err := reader.OpenStream(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, stream)
	source.calls = 0
	for range 2 {
		var data [1]byte
		if _, err := stream.Read(data[:]); err != nil || data[0] != 7 {
			t.Fatalf("read: %v %v", data, err)
		}
	}
	if source.calls != 2 || stream.file.cache.chunk != nil {
		t.Fatalf("oversized source retained: fetches=%d", source.calls)
	}
}
