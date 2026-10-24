package zstar

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"testing"
	"time"
)

// settledGoroutines polls until the goroutine count drops to want or the
// deadline passes, returning the last observed count.
func settledGoroutines(want int) int {
	deadline := time.Now().Add(5 * time.Second)
	n := runtime.NumGoroutine()
	for n > want && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		n = runtime.NumGoroutine()
	}
	return n
}

func TestContentLoadsDoNotLeakGoroutinesAfterCancellation(t *testing.T) {
	object := encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst")
	m := openThinManifest(t, ManifestOptions{ConcurrentChunks: 2})
	runtime.GC()
	baseline := runtime.NumGoroutine()
	for range 20 {
		resolver := &manifestGatedObject{data: object, entered: make(chan struct{}, 8), release: make(chan struct{})}
		// run() path: a single read canceled while the resolver is blocked.
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 2)
		go func() { _, err := m.Read(ctx, "hello.txt", 0, 6, resolver, true); done <- err }()
		<-resolver.entered
		// readMany path.
		go func() {
			_, err := m.ReadMany(ctx, []ReadRequest{{Path: "hello.txt", Length: 6}, {Path: "world.txt", Length: 6}}, resolver, true)
			done <- err
		}()
		cancel()
		for range 2 {
			<-done //nolint:errcheck // Only completion matters here; both outcomes are valid.
		}
		close(resolver.release)
		waitManifestCondition(t, func() bool {
			m.content.mu.Lock()
			defer m.content.mu.Unlock()
			return len(m.content.flights) == 0 && len(m.content.slots) == 0
		})
	}
	if n := settledGoroutines(baseline); n > baseline {
		buf := make([]byte, 1<<16)
		t.Fatalf("goroutines leaked: baseline=%d now=%d\n%s", baseline, n, buf[:runtime.Stack(buf, true)])
	}
}

func TestArchiveFileSeekReadAtWriteTo(t *testing.T) {
	r := testAPIReader(t, false)
	f, err := r.Open("dir/a")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }() //nolint:errcheck // Test cleanup.
	seeker, ok := f.(io.ReadSeeker)
	readerAt, ok2 := f.(io.ReaderAt)
	writerTo, ok3 := f.(io.WriterTo)
	if !ok || !ok2 || !ok3 {
		t.Fatal("missing interfaces")
	}
	if pos, err := seeker.Seek(1, io.SeekStart); err != nil || pos != 1 {
		t.Fatal(pos, err)
	}
	var out bytes.Buffer
	if n, err := writerTo.WriteTo(&out); err != nil || n != 2 || out.String() != "bc" {
		t.Fatalf("WriteTo: %d %v %q", n, err, out.String())
	}
	b := make([]byte, 2)
	if n, err := readerAt.ReadAt(b, 0); err != nil || n != 2 || string(b) != "ab" {
		t.Fatalf("ReadAt: %d %v %q", n, err, b)
	}
	if pos, err := seeker.Seek(-1, io.SeekEnd); err != nil || pos != 2 {
		t.Fatal(pos, err)
	}
	if _, err := seeker.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("negative seek accepted")
	}
	dir, err := r.Open("dir")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()                               //nolint:errcheck // Test cleanup.
	if _, err := dir.(io.Seeker).Seek(0, io.SeekStart); err == nil { //nolint:errcheck // Type assertion is the point of the test.
		t.Fatal("directory seek accepted")
	}
}
