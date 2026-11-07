package zstar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

type manifestGatedObject struct {
	data    []byte
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (s *manifestGatedObject) GetEncoded(context.Context, [32]byte, uint64, uint64) ([]byte, error) {
	s.calls.Add(1)
	if s.entered != nil {
		s.entered <- struct{}{}
	}
	if s.release != nil {
		<-s.release
	}
	return bytes.Clone(s.data), nil
}

func openThinManifest(t *testing.T, options ManifestReaderOptions) *manifest {
	t.Helper()
	data, err := os.ReadFile("../testdata/entry-manifest/thin.zstar")
	if err != nil {
		t.Fatal(err)
	}
	m, err := openManifest(context.Background(), memoryRange(data), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { testClose(t, m) })
	return m
}

func encodedFixture(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile("../testdata/entry-manifest/" + path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func waitManifestCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestConcurrentUncheckedResolverMissesDoNotCoalesce(t *testing.T) {
	m := openThinManifest(t, ManifestReaderOptions{ConcurrentChunks: 2})
	a := &manifestGatedObject{data: encodedFixture(t, "invalid/object-wrong-hash.zst"), entered: make(chan struct{}, 1), release: make(chan struct{})}
	b := &manifestGatedObject{data: encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst"), entered: make(chan struct{}, 1), release: make(chan struct{})}
	var onceA, onceB sync.Once
	defer onceA.Do(func() { close(a.release) })
	defer onceB.Do(func() { close(b.release) })
	errors := make(chan error, 2)
	for _, test := range []struct {
		resolver *manifestGatedObject
		expected string
	}{{a, "wrong\n"}, {b, "hello\n"}} {
		go func() {
			data, err := m.Read(context.Background(), "hello.txt", 0, 6, test.resolver, false)
			if err == nil && string(data) != test.expected {
				err = fmt.Errorf("got %q", data)
			}
			errors <- err
		}()
	}
	for _, entered := range []chan struct{}{a.entered, b.entered} {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("distinct resolver joined another namespace's flight")
		}
	}
	onceA.Do(func() { close(a.release) })
	onceB.Do(func() { close(b.release) })
	for range 2 {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
}

func TestManifestConcurrentMissCancellationAndBoundedDetachedWork(t *testing.T) {
	m := openThinManifest(t, ManifestReaderOptions{ConcurrentChunks: 1})
	resolver := &manifestGatedObject{data: encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst"), entered: make(chan struct{}, 3), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, err := m.Read(ctx, "hello.txt", 0, 6, resolver, true); first <- err }()
	<-resolver.entered
	second := make(chan error, 1)
	go func() {
		got, err := m.Read(context.Background(), "world.txt", 0, 6, resolver, true)
		if err == nil && string(got) != "world\n" {
			err = errors.New("bad bytes")
		}
		second <- err
	}()
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
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(resolver.release)
	if err := <-second; err != nil || resolver.calls.Load() != 1 {
		t.Fatalf("shared miss canceled remaining waiter: %v calls=%d", err, resolver.calls.Load())
	}
	// A canceled noninterruptible callback continues holding the single slot.
	resolver.release = make(chan struct{})
	ctx, cancel = context.WithCancel(context.Background())
	go func() { _, err := m.Read(ctx, "hello.txt", 0, 6, resolver, true); first <- err }()
	<-resolver.entered
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	go func() { _, err := m.Read(context.Background(), "world.txt", 0, 6, resolver, true); second <- err }()
	waitManifestCondition(t, func() bool { m.content.mu.Lock(); defer m.content.mu.Unlock(); return len(m.content.flights) == 1 })
	if resolver.calls.Load() != 2 {
		t.Fatal("canceled callback released work slot")
	}
	testClose(t, m)
	if err := <-second; ErrorCategory(err) != "unsupported-operation" {
		t.Fatalf("close did not unblock waiter: %v", err)
	}
	close(resolver.release)
	waitManifestCondition(t, func() bool { return len(m.content.slots) == 0 })
}

func TestManifestBatchGroupingBoundsAndFileStreams(t *testing.T) {
	ctx := context.Background()
	m := openThinManifest(t, ManifestReaderOptions{MaxReadBytes: 32})
	resolver := &manifestGatedObject{data: encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst")}
	requests := []ReadRequest{{"world.txt", 0, 6}, {"repeat", 0, 12}, {"hello.txt", 0, 6}}
	empty, report, err := m.ReadManyWithReport(ctx, []ReadRequest{{"repeat", 12, 0}, {"hello.txt", 0, 0}}, nil, true)
	if err != nil || len(empty) != 2 || len(empty[0]) != 0 || len(empty[1]) != 0 || !report.MetadataValidated || resolver.calls.Load() != 0 {
		t.Fatalf("zero-length batch: %q %#v %v", empty, report, err)
	}
	got, report, err := m.ReadManyWithReport(ctx, requests, resolver, true)
	if err != nil || len(got) != 3 || string(got[0]) != "world\n" || string(got[1]) != "hello\nhello\n" || string(got[2]) != "hello\n" || resolver.calls.Load() != 1 {
		t.Fatalf("grouped batch: %q %v calls=%d", got, err, resolver.calls.Load())
	}
	got[1][0] = 'X'
	if string(got[2]) != "hello\n" || report.FullArchiveValidated || len(report.ExternalObjectsSHA256Checked) != 1 {
		t.Fatal("ownership or scope")
	}
	if _, err = m.ReadMany(ctx, []ReadRequest{{"hello.txt", 0, 6}, {"world.txt", 7, 0}}, resolver, true); ErrorCategory(err) != "invalid-range" || resolver.calls.Load() != 1 {
		t.Fatal("invalid batch performed I/O")
	}
	if _, err = m.ReadMany(ctx, []ReadRequest{{"repeat", 0, 12}, {"repeat", 0, 12}, {"repeat", 0, 12}}, resolver, true); ErrorCategory(err) != "resource-limit" || resolver.calls.Load() != 1 {
		t.Fatal("over-budget batch performed I/O")
	}
	file, err := m.OpenFile(ctx, "repeat", resolver, true)
	if err != nil {
		t.Fatal(err)
	}
	var streamed bytes.Buffer
	if n, err := file.WriteTo(&streamed); err != nil || n != 12 || streamed.String() != "hello\nhello\n" || resolver.calls.Load() != 2 {
		t.Fatalf("stream repeated object: %d %q %v", n, streamed.String(), err)
	}
	buffer := make([]byte, 4)
	if n, err := file.ReadAt(buffer, 10); n != 2 || !errors.Is(err, io.EOF) || string(buffer[:2]) != "o\n" {
		t.Fatalf("ReaderAt EOF: %d %q %v", n, buffer, err)
	}
	testClose(t, m)
	if _, err := file.WriteTo(io.Discard); err == nil {
		t.Fatal("closed file streamed")
	}
}

func TestManifestBatchValidatesWholeSelectedExtent(t *testing.T) {
	codec, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, codec)
	raw := []byte("hello")
	address := fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
	var out bytes.Buffer
	out.Write(make([]byte, 1000))
	w, err := newManifestWriter(&out, 1000, ManifestWriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.AddEntry(Entry{Path: "file", Mode: 420, Extents: []Extent{{Size: 6, Source: mustObjectSource(t, address), EncodedSize: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := openManifest(context.Background(), memoryRange(out.Bytes()), ManifestReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, m)
	resolver := &manifestGatedObject{data: codec.EncodeAll(raw, nil)}
	if _, err = m.ReadMany(context.Background(), []ReadRequest{{"file", 0, 1}}, resolver, true); ErrorCategory(err) != "invalid-range" {
		t.Fatalf("selected extent bounds ignored: %v", err)
	}
}
