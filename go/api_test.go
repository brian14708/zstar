package zstar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"testing"
	"testing/fstest"
	"time"
)

func testAPIReader(t *testing.T, symlink bool) *Reader {
	t.Helper()
	var output bytes.Buffer
	w, err := NewWriter(&output, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, header := range []Header{
		{Path: "dir", Kind: KindDirectory, Mode: 0o755},
		{Path: "dir/a", Size: 3, Mode: 0o6750, MtimeNS: -123},
		{Path: "dir/nested", Kind: KindDirectory, Mode: 0o755},
		{Path: "dir/nested/b", Size: 3, Mode: 0o644},
		{Path: "z", Size: 3, Mode: 0o644},
	} {
		var body io.Reader
		if header.Kind != KindDirectory {
			body = bytes.NewBufferString("abc")
		}
		if err := w.Add(context.Background(), header, body); err != nil {
			t.Fatal(err)
		}
	}
	if symlink {
		if err := w.Add(context.Background(), Header{Path: "link", Kind: KindSymlink, Target: "dir/a"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := Open(context.Background(), ReaderAtSource{Reader: bytes.NewReader(output.Bytes()), Length: int64(output.Len())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}

func TestArchiveFS(t *testing.T) {
	r := testAPIReader(t, false)
	if err := fstest.TestFS(r, "dir", "dir/a", "dir/nested", "dir/nested/b", "z"); err != nil {
		t.Fatal(err)
	}
	data, err := fs.ReadFile(r, "dir/a")
	if err != nil || string(data) != "abc" {
		t.Fatalf("ReadFile: %q, %v", data, err)
	}
	sub, err := fs.Sub(r, "dir")
	if err != nil {
		t.Fatal(err)
	}
	if data, err := fs.ReadFile(sub, "nested/b"); err != nil || string(data) != "abc" {
		t.Fatalf("sub ReadFile: %q, %v", data, err)
	}
	info, err := fs.Stat(r, "dir/a")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode() != 0o750|fs.ModeSetuid|fs.ModeSetgid || !info.ModTime().Equal(time.Unix(0, -123)) {
		t.Fatalf("file metadata: %v, %v", info.Mode(), info.ModTime())
	}
	owned := mustTestValue[*Entry](t, info.Sys())
	owned.Path = "changed"
	if mustTestValue[*Entry](t, info.Sys()).Path != "dir/a" {
		t.Fatal("Sys metadata was not isolated")
	}
	f, err := r.Open("dir")
	if err != nil {
		t.Fatal(err)
	}
	dir, ok := f.(fs.ReadDirFile)
	if !ok {
		t.Fatal("directory lacks ReadDir")
	}
	for _, name := range []string{"a", "nested"} {
		entries, err := dir.ReadDir(1)
		if err != nil || len(entries) != 1 || entries[0].Name() != name {
			t.Fatalf("ReadDir: %v, %v", entries, err)
		}
	}
	if entries, err := dir.ReadDir(1); len(entries) != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("directory EOF: %v, %v", entries, err)
	}
	if entries, err := dir.ReadDir(-1); len(entries) != 0 || err != nil {
		t.Fatalf("directory remainder: %v, %v", entries, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Stat(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("closed handle: %v", err)
	}
}

func TestArchivePathErrorsAndLinks(t *testing.T) {
	r := testAPIReader(t, true)
	for _, name := range []string{"missing", "dir/missing"} {
		for _, operation := range []func() error{
			func() error { _, err := r.Entry(name); return err },
			func() error { _, err := r.Open(name); return err },
			func() error { _, err := r.OpenFile(context.Background(), name); return err },
			func() error { _, err := r.ReadLink(name); return err },
			func() error { _, err := r.Read(context.Background(), name, 0, 0); return err },
		} {
			err := operation()
			pe, ok := errors.AsType[*fs.PathError](err)
			if !errors.Is(err, fs.ErrNotExist) || !ok || pe.Path != name {
				t.Fatalf("missing path %q: %v", name, err)
			}
		}
	}
	for _, name := range []string{"", "/dir/a", "../dir/a", "dir//a"} {
		if _, err := r.Open(name); !errors.Is(err, fs.ErrInvalid) {
			t.Fatalf("invalid path %q: %v", name, err)
		}
	}
	if target, err := fs.ReadLink(r, "link"); err != nil || target != "dir/a" {
		t.Fatalf("ReadLink: %q, %v", target, err)
	}
	if _, err := r.Open("link"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("opened symlink: %v", err)
	}
	if info, err := fs.Lstat(r, "link"); err != nil || info.Mode()&fs.ModeSymlink == 0 {
		t.Fatalf("symlink Stat: %v, %v", info, err)
	}
	if _, err := fs.ReadLink(r, "z"); err == nil {
		t.Fatal("regular file treated as symlink")
	} else if _, ok := errors.AsType[*fs.PathError](err); !ok {
		t.Fatalf("ReadLink error type: %T", err)
	}
	f, err := r.Open("dir/a")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = f.Read(make([]byte, 1))
	if pe, ok := errors.AsType[*fs.PathError](err); !ok || pe.Path != "dir/a" || !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("closed nested file error: %v", err)
	}
}

func TestFileContextsApplyPerOperation(t *testing.T) {
	r := testAPIReader(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	f, err := r.OpenFile(ctx, "dir/a")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	// Caller-owned buffers can exceed the budget for SDK-returned allocations.
	r.manifest.maxRead = 1
	buffer := make([]byte, 3)
	if n, err := f.ReadAt(buffer, 0); err != nil || n != 3 || string(buffer) != "abc" {
		t.Fatalf("opening context retained: %d, %v", n, err)
	}
	if _, err := f.ReadAtContext(ctx, buffer, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("read cancellation: %v", err)
	}
	if _, err := f.WriteToContext(ctx, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("stream cancellation: %v", err)
	}
	if n, err := f.WriteTo(io.Discard); err != nil || n != 3 {
		t.Fatalf("later stream: %d, %v", n, err)
	}
}

func TestProviderCancellationIdentity(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		r := &RefreshingRangeReader{Provider: func(context.Context, string, int64, int64, bool) (string, http.Header, error) {
			return "", nil, fmt.Errorf("secret URL: %w", cause)
		}}
		_, err := r.ReadRange(context.Background(), 0, 1)
		if !errors.Is(err, cause) || ErrorCategory(err) != "cancelled" || err.Error() != cause.Error() {
			t.Fatalf("provider error: %v", err)
		}
		err = PutPresigned(context.Background(), func(context.Context, bool) (string, http.Header, error) {
			return "", nil, fmt.Errorf("secret PUT URL: %w", cause)
		}, nil, nil)
		if !errors.Is(err, cause) || err.Error() != cause.Error() {
			t.Fatalf("PUT provider error: %v", err)
		}
	}
}

func TestExtentSources(t *testing.T) {
	source := ArchiveSource(123)
	if offset, ok := source.ArchiveOffset(); !ok || offset != 123 {
		t.Fatal("archive offset")
	}
	if _, ok := source.ContentAddress(); ok {
		t.Fatal("archive source has an object address")
	}
	object := ObjectSource([32]byte{31: 7})
	address, ok := object.ContentAddress()
	if !ok {
		t.Fatal("missing object address")
	}
	parsed, err := ParseObjectSource(address)
	if err != nil || parsed != object {
		t.Fatalf("object round trip: %v, %v", parsed, err)
	}
	if _, err := ParseObjectSource("sha256:BAD"); err == nil {
		t.Fatal("invalid object address accepted")
	}
}

func TestArchiveDirectorySubtreeBoundaries(t *testing.T) {
	ctx := context.Background()
	var output bytes.Buffer
	w := mustTestWriter(t, &output, WriterOptions{})
	for _, h := range []Header{
		{Path: "a", Kind: KindDirectory},
		{Path: "a-keep"},
		{Path: "a/child", Kind: KindDirectory},
		{Path: "a/child-keep"},
		{Path: "a/child/grandchild"},
		{Path: "a0keep"},
	} {
		if err := w.Add(ctx, h, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := Open(ctx, memoryRange(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, r)
	if err := fstest.TestFS(r, "a", "a-keep", "a/child", "a/child-keep", "a/child/grandchild", "a0keep"); err != nil {
		t.Fatal(err)
	}
}
