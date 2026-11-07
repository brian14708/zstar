package zstar

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"testing"
)

func TestFileDecodedCacheOwnershipReplacementAndLifetime(t *testing.T) {
	ctx := t.Context()
	source := &manifestCountedRange{memoryRange: buildTestArchive(t, 1024)}
	reader, err := Open(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, reader)
	file, err := reader.OpenFile(ctx, "nested/data")
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, file)
	source.calls = 0
	for _, offset := range []int64{0, 1, 5999, 0} {
		var out [1]byte
		if _, err := file.ReadAtContext(ctx, out[:], offset); err != nil || out[0] != "abcd"[offset%4] {
			t.Fatalf("read at %d: %v %q", offset, err, out)
		}
		out[0] = 99 // Mutating a caller buffer must not corrupt decoded retention.
	}
	if source.calls != 3 {
		t.Fatalf("replacement: %d fetches", source.calls)
	}
	other, err := reader.OpenFile(ctx, "nested/data")
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, other)
	var out [1]byte
	if _, err := other.ReadAt(out[:], 0); err != nil {
		t.Fatal(err)
	}
	if source.calls != 4 {
		t.Fatal("separate handles shared completed decoded cache")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := file.ReadAtContext(cancelled, out[:], 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached cancellation: %v", err)
	}
	testClose(t, file)
	testClose(t, file)
	if file.cache.chunk != nil {
		t.Fatal("close retained decoded source")
	}
	if _, err := file.ReadAt(out[:], 0); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("closed file: %v", err)
	}
	if _, err := other.ReadAt(out[:], 0); err != nil {
		t.Fatalf("close affected other handle: %v", err)
	}
	testClose(t, reader)
	if _, err := other.ReadAt(out[:], 0); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("cached read ignored archive close: %v", err)
	}
}

func TestFileDecodedCacheCrossExtentAndWriteTo(t *testing.T) {
	ctx := t.Context()
	source := &manifestCountedRange{memoryRange: buildTestArchive(t, 1<<20)}
	reader, err := Open(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, reader)
	entry, err := reader.Entry("nested/data")
	if err != nil {
		t.Fatal(err)
	}
	if len(entry.Extents) != 1 {
		t.Fatal("fixture must fit one frame")
	}
	first := entry.Extents[0]
	second := first
	first.Size = 3000
	second.Offset += first.Size
	second.Size -= first.Size
	entry.Extents = []Extent{first, second}
	file, err := reader.manifest.openEntryFile(entry, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, file)
	source.calls = 0
	output := make([]byte, 32)
	for range 2 {
		if _, err := file.ReadAtContext(ctx, output, 2992); err != nil || !bytes.Equal(output, bytes.Repeat([]byte("abcd"), 8)) {
			t.Fatalf("cross-extent: %q %v", output, err)
		}
	}
	var whole bytes.Buffer
	if n, err := file.WriteToContext(ctx, &whole); err != nil || n != 6000 {
		t.Fatalf("WriteTo: %d %v", n, err)
	}
	if !bytes.Equal(whole.Bytes(), bytes.Repeat([]byte("abcd"), 1500)) || source.calls != 1 {
		t.Fatalf("batch cache reuse: fetches=%d", source.calls)
	}
	// The second extent now selects past the source. A cache hit must reject it.
	file.entry.Extents[1].Offset = uint64(len(file.cache.chunk.raw))
	if _, err := file.ReadAt(output[:1], 3000); err == nil {
		t.Fatal("cached frame bypassed extent bounds")
	}
}

func TestFileDecodedCacheSkipsOversizedSources(t *testing.T) {
	ctx := t.Context()
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
	file, err := reader.OpenFile(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, file)
	source.calls = 0
	for range 2 {
		var out [1]byte
		if _, err := file.ReadAt(out[:], 0); err != nil || out[0] != 7 {
			t.Fatalf("oversized read: %q %v", out, err)
		}
	}
	if source.calls != 2 || file.cache.chunk != nil {
		t.Fatal("oversized decoded source retained")
	}
}

func TestFileDecodedCacheConcurrentReadsAndClose(t *testing.T) {
	for _, closeDuringLoad := range []bool{false, true} {
		t.Run(map[bool]string{false: "shared", true: "close"}[closeDuringLoad], func(t *testing.T) {
			m := openThinManifest(t, ManifestReaderOptions{ConcurrentChunks: 2})
			resolver := &manifestGatedObject{data: encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst"), entered: make(chan struct{}, 2), release: make(chan struct{})}
			file, err := m.OpenFile(t.Context(), "hello.txt", resolver, true)
			if err != nil {
				t.Fatal(err)
			}
			defer testClose(t, file)
			results := make(chan error, 2)
			read := func() {
				var out [6]byte
				_, err := file.ReadAtContext(t.Context(), out[:], 0)
				if err == nil && string(out[:]) != "hello\n" {
					err = errors.New("incorrect decoded bytes")
				}
				results <- err
			}
			go read()
			<-resolver.entered
			go read()
			waitManifestCondition(t, func() bool {
				m.content.mu.Lock()
				defer m.content.mu.Unlock()
				for _, flight := range m.content.flights {
					if flight.waiters == 2 {
						return true
					}
				}
				return false
			})
			if closeDuringLoad {
				testClose(t, file)
			}
			close(resolver.release)
			for range 2 {
				err := <-results
				if closeDuringLoad {
					if !errors.Is(err, fs.ErrClosed) {
						t.Fatalf("close during load: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
			if resolver.calls.Load() != 1 {
				t.Fatal("concurrent reads repeated decoding")
			}
			if closeDuringLoad && file.cache.chunk != nil {
				t.Fatal("load repopulated a closed cache")
			}
		})
	}
}

func TestFilesystemCloseReleasesDecodedCache(t *testing.T) {
	reader, err := Open(t.Context(), memoryRange(buildTestArchive(t, 1024)))
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, reader)
	handle, err := reader.Open("nested/data")
	if err != nil {
		t.Fatal(err)
	}
	file, ok := handle.(*archiveFile)
	if !ok {
		t.Fatal("unexpected filesystem handle type")
	}
	if _, err := io.ReadFull(file, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if file.file.cache.chunk == nil {
		t.Fatal("filesystem handle did not retain decoded source")
	}
	testClose(t, handle)
	if file.file.cache.chunk != nil {
		t.Fatal("filesystem close retained decoded source")
	}
}

func TestFileDecodedCacheKeepsVerificationPolicyAndRetriesFailures(t *testing.T) {
	m := openThinManifest(t, ManifestReaderOptions{})
	wrong := encodedFixture(t, "invalid/object-wrong-hash.zst")
	correct := encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst")
	resolver := &manifestGatedObject{data: wrong}
	verified, err := m.OpenFile(t.Context(), "hello.txt", resolver, true)
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, verified)
	unchecked, err := m.OpenFile(t.Context(), "hello.txt", resolver, false)
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, unchecked)
	var out [6]byte
	if _, err := unchecked.ReadAt(out[:], 0); err != nil || string(out[:]) != "wrong\n" {
		t.Fatalf("unchecked read: %q %v", out, err)
	}
	if _, err := verified.ReadAt(out[:], 0); err == nil {
		t.Fatal("verified handle reused unchecked source")
	}
	if verified.cache.chunk != nil {
		t.Fatal("failed verification retained a source")
	}
	resolver.data = correct
	for range 2 {
		if _, err := verified.ReadAt(out[:], 0); err != nil || string(out[:]) != "hello\n" {
			t.Fatalf("retry/cache: %q %v", out, err)
		}
	}
	if resolver.calls.Load() != 3 {
		t.Fatalf("verification retry/cache: %d calls", resolver.calls.Load())
	}
}
