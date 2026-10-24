package zstar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestManifestBlocksSpillHashAndSearch(t *testing.T) {
	var output bytes.Buffer
	output.Write(make([]byte, 1000))
	writer, err := NewManifestWriter(&output, 1000, ManifestOptions{MemoryBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	for i := 299; i >= 0; i-- {
		entry := Entry{Path: fmt.Sprintf("file-%04d", i), Type: 0, Mode: 420, Extents: []Extent{}, Attributes: Extensions{"x-padding": bytes.Repeat([]byte{7}, 4096)}}
		if err = writer.AddEntry(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	store := &failingCloseSpill{}
	manifest, err := OpenManifest(context.Background(), memoryRange(output.Bytes()), ManifestOptions{
		MemoryBytes:      64,
		CreateSpillStore: func() (ManifestSpillStore, error) { return store, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, manifest)
	if store.Len() == 0 || !store.closed {
		t.Fatal("metadata did not spill or its store was not closed after indexing")
	}
	if err := manifest.ValidateMetadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	entry, err := manifest.Entry("file-0299")
	if err != nil || entry == nil || entry.Path != "file-0299" {
		t.Fatalf("lookup: %v %v", entry, err)
	}
	entry, err = manifest.Entry("file-0300")
	if !errors.Is(err, fs.ErrNotExist) || entry != nil {
		t.Fatalf("missing lookup: %v %v", entry, err)
	}
	count := 0
	if err = manifest.VisitEntries(func(Entry) error { count++; return nil }); err != nil || count != 300 {
		t.Fatalf("iteration: %d %v", count, err)
	}
	bad := bytes.Clone(output.Bytes())
	bad[len(bad)-1] ^= 1
	if _, err = OpenManifest(context.Background(), memoryRange(bad), ManifestOptions{}); err == nil {
		t.Fatal("bad manifest hash accepted")
	}
}

func TestRootExtensionNestingAtWireDepth(t *testing.T) {
	for _, depth := range []int{63, 64} {
		t.Run(strconv.Itoa(depth), func(t *testing.T) {
			var value any = true
			for range depth {
				value = []any{value}
			}
			var output bytes.Buffer
			output.WriteByte(0)
			writer, err := NewManifestWriter(&output, 1, ManifestOptions{
				RootExtensions: Extensions{"x-depth": value},
			})
			if depth == 64 {
				if err == nil {
					testClose(t, writer)
					t.Fatal("over-nested root extension accepted")
				}
				if output.Len() != 1 {
					t.Fatal("invalid header emitted metadata")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = writer.Close(); err != nil {
				t.Fatal(err)
			}
			manifest, err := OpenManifest(context.Background(), memoryRange(output.Bytes()), ManifestOptions{})
			if err != nil {
				t.Fatal(err)
			}
			testClose(t, manifest)
		})
	}
}

func TestManifestEntryOwnershipAndCursor(t *testing.T) {
	var out bytes.Buffer
	out.Write(make([]byte, 1000))
	w, err := NewManifestWriter(&out, 1000, ManifestOptions{BlockTarget: 512})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if err = w.AddEntry(Entry{Path: fmt.Sprintf("f%04d", i), Mode: 420, Extents: []Extent{{Size: 1, Source: ArchiveSource(0), EncodedSize: 1}}, Attributes: Extensions{"x-bytes": []byte{7}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := OpenManifest(context.Background(), memoryRange(out.Bytes()), ManifestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, m)
	entry, err := m.Entry("f0020")
	if err != nil {
		t.Fatal(err)
	}
	entry.Extents[0].Size = 9
	mustTestValue[[]byte](t, entry.Attributes["x-bytes"])[0] = 9
	again, err := m.Entry("f0020")
	if err != nil || again.Extents[0].Size != 1 || mustTestValue[[]byte](t, again.Attributes["x-bytes"])[0] != 7 {
		t.Fatalf("entry ownership: %v %v", again, err)
	}
	for i := range 100 {
		path := fmt.Sprintf("f%04d", i)
		entry, err := m.Entry(path)
		if err != nil || entry == nil || entry.Path != path {
			t.Fatalf("lookup %s: %v %v", path, entry, err)
		}
	}
	cursor := m.Cursor()
	count, batchSize := 0, 1
	for {
		batch, err := cursor.Next(batchSize)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) != min(batchSize, 100-count) {
			t.Fatalf("cursor batch at %d: got %d entries", count, len(batch))
		}
		if len(batch) == 0 {
			break
		}
		for _, entry := range batch {
			if entry.Path != fmt.Sprintf("f%04d", count) {
				t.Fatalf("cursor entry %d: %s", count, entry.Path)
			}
			count++
		}
		batchSize = 32
	}
	if count != 100 {
		t.Fatalf("cursor count %d", count)
	}
	if err = m.ValidateMetadata(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManifestArchiveEncoderSharesAndSplitsFrames(t *testing.T) {
	ctx := context.Background()
	var out bytes.Buffer
	writer, err := NewEntryArchiveWriter(&out, 2048, ManifestOptions{MemoryBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []struct {
		name string
		data []byte
	}{{"a", []byte("hello")}, {"b", []byte("world")}, {"c", bytes.Repeat([]byte{7}, 10000)}} {
		if err := writer.Add(ctx, Entry{Path: file.name, Type: 0, Mode: 420}, uint64(len(file.data)), bytes.NewReader(file.data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := OpenManifest(ctx, memoryRange(out.Bytes()), ManifestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, result)
	if err := result.ValidateMetadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	a, err := result.Entry("a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := result.Entry("b")
	if err != nil {
		t.Fatal(err)
	}
	if a.Extents[0].Source != b.Extents[0].Source {
		t.Fatal("small files do not share a frame")
	}
	data, err := result.Read(ctx, "c", 0, 10000, nil, true)
	if err != nil || !bytes.Equal(data, bytes.Repeat([]byte{7}, 10000)) {
		t.Fatalf("split read: %v", err)
	}
}

func TestManifestContentReferences(t *testing.T) {
	values := []Extent{{Size: 4, Source: ObjectSource([32]byte{31: 7}), EncodedSize: 1, Offset: 9}}
	raw, err := EncodeContentReference(values)
	if err != nil {
		t.Fatal(err)
	}
	extents, err := DecodeContentReference(raw)
	if err != nil || len(extents) != 1 || extents[0] != values[0] {
		t.Fatalf("reference: %v", err)
	}
	pointer, err := EncodeContentPointer(values)
	if err != nil {
		t.Fatal(err)
	}
	extents, err = DecodeContentPointer(pointer)
	if err != nil || len(extents) != 1 || extents[0] != values[0] {
		t.Fatalf("pointer: %v", err)
	}
	if _, err := DecodeContentPointer(bytes.Replace(pointer, []byte(" 1 9\n"), []byte(" 1 0\n"), 1)); err == nil {
		t.Fatal("explicit default accepted")
	}
}

func TestManifestExtentDefaults(t *testing.T) {
	extent := Extent{Size: 5, Source: ArchiveSource(8192), EncodedSize: 800}
	if len(extent.value()) != 3 {
		t.Fatal("zero offset not omitted")
	}
	if _, err := parseExtent([]any{uint64(5), uint64(8192), uint64(800), uint64(0)}, 9000); err == nil {
		t.Fatal("explicit zero offset accepted")
	}
	object := Extent{Size: 5, Source: ObjectSource([32]byte{31: 7}), EncodedSize: 1, Offset: 100}
	if err := object.validate(0); err != nil {
		t.Fatal(err)
	}
}

type countedEncodedObject struct {
	data  []byte
	calls int
}

func (r *countedEncodedObject) GetEncoded(context.Context, [32]byte, uint64, uint64) ([]byte, error) {
	r.calls++
	return bytes.Clone(r.data), nil
}

func TestManifestAdjacentExtentsReuseObjectAndCheckEveryRange(t *testing.T) {
	raw := []byte("hello world")
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, encoder)
	resolver := &countedEncodedObject{data: encoder.EncodeAll(raw, nil)}
	address := fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
	var out bytes.Buffer
	out.Write(make([]byte, 1000))
	writer, err := NewManifestWriter(&out, 1000, ManifestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		offset := uint64(6)
		if name == "b" {
			offset = 10
		}
		err = writer.AddEntry(Entry{Path: name, Mode: 420, Extents: []Extent{
			{Size: 5, Source: mustObjectSource(t, address), EncodedSize: 1},
			{Size: 5, Source: mustObjectSource(t, address), EncodedSize: 2, Offset: offset},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := OpenManifest(context.Background(), memoryRange(out.Bytes()), ManifestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, m)
	data, err := m.Read(context.Background(), "a", 0, 10, resolver, true)
	if err != nil || string(data) != "helloworld" || resolver.calls != 1 {
		t.Fatalf("reuse: %q %v calls=%d", data, err, resolver.calls)
	}
	if _, err = m.Read(context.Background(), "b", 0, 10, resolver, true); err == nil {
		t.Fatal("cached object bypassed extent bounds")
	}
	if resolver.calls != 2 {
		t.Fatal("per-read reuse fetched repeated extents")
	}
	if _, err = m.Read(context.Background(), "a", 3, 0, resolver, true); err != nil || resolver.calls != 2 {
		t.Fatal("empty read fetched content")
	}
}

func TestSequenceHeaderLargerThanSegment(t *testing.T) {
	var out bytes.Buffer
	out.WriteByte(0)
	padding := bytes.Repeat([]byte{7}, (16<<20)+1)
	w, err := NewManifestWriter(&out, 1, ManifestOptions{RootExtensions: Extensions{"x-large": padding}})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := OpenManifest(context.Background(), memoryRange(out.Bytes()), ManifestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, m)
	if m.count != 0 || !bytes.Equal(mustTestValue[[]byte](t, m.RootExtensions()["x-large"]), padding) {
		t.Fatal("large empty header changed")
	}
}

func TestSortingReleasesSpillsAfterSuccessAndFailure(t *testing.T) {
	for _, failure := range []string{"", "duplicate", "ancestor", "quota"} {
		t.Run(failure, func(t *testing.T) {
			limit := uint64(8 << 20)
			if failure == "quota" {
				limit = 400000
			}
			budget := NewSpoolBudget(limit)
			stores := []*failingCloseSpill{}
			var out bytes.Buffer
			w, err := NewManifestWriter(&out, 1, ManifestOptions{
				MemoryBytes: 1, SpoolBudget: budget,
				CreateSpillStore: func() (ManifestSpillStore, error) {
					s := &failingCloseSpill{}
					stores = append(stores, s)
					return s, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			for i := 299; i >= 0; i-- {
				if err = w.AddEntry(Entry{Path: fmt.Sprintf("f%04d", i), Mode: 420, Extents: []Extent{}, Attributes: Extensions{"x-padding": make([]byte, 1024)}}); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "duplicate" || failure == "ancestor" {
				path := "f0000"
				if failure == "ancestor" {
					path = "missing/child"
				}
				if err = w.AddEntry(Entry{Path: path, Mode: 420, Extents: []Extent{}}); err != nil {
					t.Fatal(err)
				}
			}
			err = w.Close()
			if (err == nil) != (failure == "") {
				t.Fatalf("finish: %v", err)
			}
			for _, store := range stores {
				if !store.closed {
					t.Fatal("unclosed sort store")
				}
			}
			if budget.Used() != 0 {
				t.Fatal("sort quota retained")
			}
		})
	}
}
