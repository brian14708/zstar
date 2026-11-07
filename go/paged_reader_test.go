package zstar

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type pagedStalledSource struct {
	memoryRange
	stall    atomic.Bool
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

func (s *pagedStalledSource) ReadRange(ctx context.Context, offset, length int64) ([]byte, error) {
	if !s.stall.Load() {
		return s.memoryRange.ReadRange(ctx, offset, length)
	}
	close(s.entered)
	select {
	case <-ctx.Done():
		close(s.canceled)
		<-s.release
		return nil, ctx.Err()
	case <-s.release:
		return nil, context.Canceled
	}
}

func receivePagedResult[T any](t *testing.T, result <-chan T, operation string) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
		var zero T
		return zero
	}
}

func TestPagedCloseCancelsMetadata(t *testing.T) {
	for _, test := range []struct {
		name string
		load func(*Reader) error
	}{
		{"entry", func(r *Reader) error { _, err := r.Entry("a/file"); return err }},
		{"full scan", func(r *Reader) error { _, err := r.EntryCount(); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := &pagedStalledSource{
				memoryRange: encodedFixture(t, "pages/cross-page-ancestors.zstar"),
				entered:     make(chan struct{}),
				canceled:    make(chan struct{}),
				release:     make(chan struct{}),
			}
			var release sync.Once
			unblock := func() { release.Do(func() { close(source.release) }) }
			t.Cleanup(unblock)
			reader, err := Open(context.Background(), source)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { unblock(); testClose(t, reader) })
			source.stall.Store(true)
			loaded := make(chan error, 1)
			go func() { loaded <- test.load(reader) }()
			receivePagedResult(t, source.entered, "metadata request")

			closed := make(chan error, 1)
			go func() { closed <- reader.Close() }()
			receivePagedResult(t, source.canceled, "metadata cancellation")
			// Content cancellation must not wait for the metadata transport to return.
			receivePagedResult(t, reader.manifest.content.closed, "content cancellation")
			unblock()
			if err := receivePagedResult(t, closed, "Close"); err != nil {
				t.Fatal(err)
			}
			if err := receivePagedResult(t, loaded, "metadata result"); !errors.Is(err, context.Canceled) {
				t.Fatalf("metadata load: %v", err)
			}
		})
	}
}

func TestPagedSelectiveAndCompleteMetadataScopes(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"cross-page-ancestors", "unselected-extension-checksum", "unselected-entry-checksum", "unused-external-declaration"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile("../testdata/entry-manifest/pages/" + name + ".zstar")
			if err != nil {
				t.Fatal(err)
			}
			reader, err := Open(ctx, memoryRange(data))
			if err != nil {
				t.Fatal(err)
			}
			defer testClose(t, reader)
			got, err := reader.Read(ctx, "a/file", 0, 5)
			if err != nil || string(got) != "\x00\x01\x02\x03\x04" {
				t.Fatalf("selected read: %q %v", got, err)
			}
			if _, err = reader.Read(ctx, "a/file", 0, 0); err != nil {
				t.Fatal(err)
			}
			err = reader.manifest.paged.ensureFull(ctx)
			if (err == nil) != (name == "cross-page-ancestors") {
				t.Fatalf("complete metadata: %v", err)
			}
		})
	}
}

func TestPagedCrossPageAncestorValidation(t *testing.T) {
	data, err := os.ReadFile("../testdata/entry-manifest/pages/missing-ancestor.zstar")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Open(context.Background(), memoryRange(data))
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, reader)
	if _, err = reader.Entry("a/file"); err == nil {
		t.Fatal("missing ancestor accepted")
	}
}

func TestPagedOpeningReadsOnlyFooterAndDirectory(t *testing.T) {
	ctx := context.Background()
	data := encodedFixture(t, "pages/cross-page-ancestors.zstar")
	source := &manifestCountedRange{memoryRange: data}
	reader, err := Open(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, reader)
	footer, err := ParseFooter(data[len(data)-FooterSize:])
	if err != nil {
		t.Fatal(err)
	}
	if source.calls != 2 || source.bytes != int64(FooterSize)+int64(footer.DirectoryEncodedSize) {
		t.Fatalf("opening fetched unexpected bytes: calls=%d bytes=%d", source.calls, source.bytes)
	}
}

// This caller-supplied source owns its raw cache independently of Reader.
type callerCachedRange struct {
	RangeReader
	mu      sync.Mutex
	ranges  map[[2]int64][]byte
	fetches int
}

func (s *callerCachedRange) ReadRange(ctx context.Context, at, length int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := [2]int64{at, length}
	if data, ok := s.ranges[key]; ok {
		return slices.Clone(data), nil
	}
	data, err := s.RangeReader.ReadRange(ctx, at, length)
	if err != nil {
		return nil, err
	}
	s.fetches++
	s.ranges[key] = slices.Clone(data)
	return data, nil
}

func TestCallerSourceOwnsRawCacheAcrossReaders(t *testing.T) {
	ctx := context.Background()
	source := &callerCachedRange{
		RangeReader: memoryRange(buildTestArchive(t, 1024)),
		ranges:      make(map[[2]int64][]byte),
	}
	for round := range 2 {
		before := source.fetches
		reader, err := Open(ctx, source)
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			data, err := reader.Read(ctx, "nested/data", 0, 4)
			if err != nil || string(data) != "abcd" {
				t.Fatalf("read: %q %v", data, err)
			}
			data[0] = 'z' // Returned buffers must not corrupt cached raw bytes.
		}
		testClose(t, reader)
		if round == 0 && source.fetches == before {
			t.Fatal("source was not fetched")
		}
		if round == 1 && source.fetches != before {
			t.Fatal("caller cache did not survive reader close")
		}
	}
}
