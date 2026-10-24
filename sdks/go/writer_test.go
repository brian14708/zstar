package zstar

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func buildTestArchive(t *testing.T, target int) []byte {
	t.Helper()
	var output bytes.Buffer
	writer, err := NewWriter(&output, WriterOptions{FrameSize: target})
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range []struct {
		header Header
		body   string
	}{
		{Header{Path: "empty", Mode: 0o644}, ""},
		{Header{Path: "link", Kind: "symlink", Mode: 0o777, Target: "../nested//data"}, ""},
		{Header{Path: "nested", Kind: "directory", Mode: 0o755}, ""},
		{Header{Path: "nested/data", Mode: 0o640, Size: 6000}, strings.Repeat("abcd", 1500)},
	} {
		if err = writer.Add(context.Background(), member.header, testMemberBody(member.header, member.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func TestWriterRoundTripAndReaderAt(t *testing.T) {
	for _, target := range []int{512, 1024, 4096, 1 << 20} {
		t.Run(fmtSize(target), func(t *testing.T) {
			encoded := buildTestArchive(t, target)
			archive, err := Open(context.Background(), ReaderAtSource{Reader: bytes.NewReader(encoded), Length: int64(len(encoded))})
			if err != nil {
				t.Fatal(err)
			}
			defer testClose(t, archive)
			if _, err := archive.Verify(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			want := strings.Repeat("abcd", 1500)
			got, err := archive.Read(context.Background(), "nested/data", 0, 6000)
			if err != nil || string(got) != want {
				t.Fatalf("read: %q %v", got, err)
			}
			target, err := archive.ReadLink("link")
			if err != nil || target != "../nested//data" {
				t.Fatalf("link %q %v", target, err)
			}
			if _, err = archive.OpenFile(context.Background(), "link"); err == nil {
				t.Fatal("followed symlink")
			}
			file, err := archive.OpenFile(context.Background(), "nested/data")
			if err != nil {
				t.Fatal(err)
			}
			buffer := make([]byte, 4)
			if n, err := file.ReadAt(buffer, 5998); n != 2 || !errors.Is(err, io.EOF) || string(buffer[:n]) != "cd" {
				t.Fatalf("partial EOF: %d %v %q", n, err, buffer)
			}
			if n, err := file.ReadAt(buffer, 6000); n != 0 || !errors.Is(err, io.EOF) {
				t.Fatalf("EOF: %d %v", n, err)
			}
			if _, err := file.ReadAt(buffer, -1); err == nil {
				t.Fatal("negative offset accepted")
			}
			if n, err := file.ReadAt(nil, 6000); n != 0 || err != nil {
				t.Fatalf("empty read: %d %v", n, err)
			}
			var cloned bytes.Buffer
			if n, err := file.WriteTo(&cloned); n != 6000 || err != nil || cloned.String() != want {
				t.Fatalf("stream: %d %v", n, err)
			}
			empty, err := archive.Read(context.Background(), "empty", 0, 0)
			if err != nil || len(empty) != 0 {
				t.Fatalf("empty: %v", err)
			}
			// Returned metadata must not permit callers to mutate live index bounds.
			entry, err := archive.Entry("nested/data")
			if err != nil {
				t.Fatal(err)
			}
			entry.Extents[0].Size = math.MaxUint64
			again, err := archive.Entry("nested/data")
			if err != nil {
				t.Fatal(err)
			}
			if size, err := again.Size(); err != nil || size != 6000 {
				t.Fatal("metadata aliases index")
			}
		})
	}
}
func fmtSize(size int) string { return fmt.Sprintf("frame-%d", size) }

func TestWriterPAXWithStandardTarReader(t *testing.T) {
	var output bytes.Buffer
	writer, err := NewWriter(&output, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	path := "目录-" + strings.Repeat("é", 60)
	headers := []Header{
		{Path: "long-link", Kind: "symlink", Mode: 0o777, Target: strings.Repeat("../", 80)},
		{Path: path, Mode: 0o600, UID: 1 << 40, GID: 1 << 41, MtimeNS: -1, Size: 3},
	}
	for _, header := range headers {
		if err = writer.Add(context.Background(), header, testMemberBody(header, "xyz")); err != nil {
			t.Fatal(err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	archive, err := Open(context.Background(), memoryRange(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	entry, err := archive.Entry(path)
	if err != nil {
		t.Fatal(err)
	}
	if entry.UID != 1<<40 || entry.MtimeNS != -1 {
		t.Fatal("PAX index metadata")
	}
	decoder, err := zstd.NewReader(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	tarReader := tar.NewReader(decoder)
	for _, want := range headers {
		header, err := tarReader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if header.Name != want.Path || header.Linkname != want.Target || uint64(header.Uid) != want.UID || header.ModTime.UnixNano() != want.MtimeNS {
			t.Fatalf("tar header: %+v", header)
		}
		body, err := io.ReadAll(tarReader)
		if err != nil {
			t.Fatal(err)
		}
		if want.Kind != "symlink" && string(body) != "xyz" {
			t.Fatalf("tar body: %q", body)
		}
	}
	if _, err = tarReader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("tar end: %v", err)
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func TestWriterFailureAndNamespaceRules(t *testing.T) {
	for _, destination := range []io.Writer{failWriter{}, shortWriter{}} {
		writer := mustTestWriter(t, destination, WriterOptions{FrameSize: 512})
		err := writer.Add(context.Background(), Header{Path: "x", Size: 3}, strings.NewReader("abc"))
		if err == nil || writer.Close() == nil {
			t.Fatal("output error was ignored")
		}
	}
	var output bytes.Buffer
	writer := mustTestWriter(t, &output, WriterOptions{})
	if err := writer.Add(context.Background(), Header{Path: "x", Size: 4}, strings.NewReader("abc")); err == nil || writer.Close() == nil {
		t.Fatal("truncated body was ignored")
	}
	for _, header := range []Header{{Path: "../x"}, {Path: "parent/child"}} {
		writer = mustTestWriter(t, io.Discard, WriterOptions{})
		if err := writer.Add(context.Background(), header, nil); err == nil && writer.Close() == nil {
			t.Fatal("unsafe namespace accepted")
		}
	}
	writer = mustTestWriter(t, io.Discard, WriterOptions{})
	if err := writer.Add(context.Background(), Header{Path: "x"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(context.Background(), Header{Path: "x"}, nil); err != nil {
		t.Fatal(err)
	}
	if writer.Close() == nil {
		t.Fatal("duplicate path accepted")
	}
	writer = mustTestWriter(t, io.Discard, WriterOptions{})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := writer.Add(canceled, Header{Path: "x"}, nil); !errors.Is(err, context.Canceled) || writer.Close() == nil {
		t.Fatalf("cancellation not sticky: %v", err)
	}
	writer = mustTestWriter(t, io.Discard, WriterOptions{})
	if err := writer.Add(context.Background(), Header{Path: "x", Size: 2}, strings.NewReader("abc")); err == nil || writer.Close() == nil {
		t.Fatal("extra body accepted")
	}
}

func TestSmallMembersShareFramesAndCache(t *testing.T) {
	var output bytes.Buffer
	writer := mustTestWriter(t, &output, WriterOptions{})
	for i := range 10 {
		if err := writer.Add(context.Background(), Header{Path: fmt.Sprintf("f%d", i), Size: 1}, strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	source := &countedSource{data: output.Bytes()}
	archive, err := Open(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	first, err := archive.Entry("f0")
	if err != nil {
		t.Fatal(err)
	}
	last, err := archive.Entry("f9")
	if err != nil || first.Extents[0].Source != last.Extents[0].Source {
		t.Fatal("small members scattered")
	}
	for i := range 10 {
		if _, err = archive.Read(context.Background(), fmt.Sprintf("f%d", i), 0, 1); err != nil {
			t.Fatal(err)
		}
	}
	if source.reads != 3 {
		t.Fatalf("wanted footer+metadata+shared frame, got %d reads", source.reads)
	}
}

type countedSource struct {
	data  []byte
	reads int
}

func (s *countedSource) Size(context.Context) (int64, error) { return int64(len(s.data)), nil }
func (s *countedSource) ReadRange(_ context.Context, offset, length int64) ([]byte, error) {
	s.reads++
	return append([]byte(nil), s.data[offset:offset+length]...), nil
}

func TestConcurrentReaderAtAndBudget(t *testing.T) {
	data := buildTestArchive(t, 1024)
	archive, err := OpenWithOptions(context.Background(), ReaderAtSource{Reader: bytes.NewReader(data), Length: int64(len(data))}, ReaderOptions{Manifest: ManifestOptions{MaxReadBytes: 32, ContentCacheBytes: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	file, err := archive.OpenFile(context.Background(), "nested/data")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = archive.Read(context.Background(), "nested/data", 0, 6000); err == nil {
		t.Fatal("read budget ignored")
	}
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for n := range 20 {
				p := make([]byte, 4)
				if _, err := file.ReadAt(p, int64(n*4)); err != nil || string(p) != "abcd" {
					t.Errorf("concurrent read: %q %v", p, err)
				}
			}
		})
	}
	group.Wait()
	var output bytes.Buffer
	if _, err = file.WriteTo(&output); err != nil || output.Len() != 6000 {
		t.Fatalf("bounded stream: %v", err)
	}
}

func testMemberBody(header Header, body string) io.Reader {
	if header.Kind == "directory" || header.Kind == "symlink" {
		return nil
	}
	return strings.NewReader(body)
}

func TestWriterArgumentErrorsAreNotSticky(t *testing.T) {
	var output bytes.Buffer
	writer := mustTestWriter(t, &output, WriterOptions{})
	ctx := context.Background()
	bad := []struct {
		header Header
		body   io.Reader
	}{
		{Header{Path: "dir", Kind: "directory"}, strings.NewReader("body")},
		{Header{Path: "dir", Kind: "directory", Size: 4}, strings.NewReader("body")},
		{Header{Path: "x", Kind: "bogus"}, nil},
		{Header{Path: "x", Target: "t"}, nil},
		{Header{Path: "../x"}, nil},
		{Header{Path: "l", Kind: "symlink", Target: "bad\xff"}, nil},
	}
	for i, c := range bad {
		if err := writer.Add(ctx, c.header, c.body); err == nil {
			t.Fatalf("case %d accepted", i)
		}
	}
	if err := writer.Add(ctx, Header{Path: "ok", Size: 2}, strings.NewReader("hi")); err != nil {
		t.Fatalf("writer poisoned by argument errors: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	link := "bad\xff"
	if err := (Entry{Path: "l", Type: TypeSymlink, Linkname: &link}).validateFields(0); err == nil {
		t.Fatal("non-UTF-8 linkname accepted")
	}
}
