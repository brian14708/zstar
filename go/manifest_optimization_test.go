package zstar

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"testing"
)

func TestCanonicalSequenceAcrossStorageBoundaries(t *testing.T) {
	// The integer's extended header crosses the 64 KiB boundary, and the
	// following string spans another boundary. Decoded values must survive
	// replacement of the reader's read-ahead buffer.
	values := []any{bytes.Repeat([]byte{7}, 65530), uint64(1 << 40), string(bytes.Repeat([]byte{'x'}, 70<<10))}
	for _, memoryBytes := range []uint64{1 << 20, 128} {
		t.Run(strconv.FormatUint(memoryBytes, 10), func(t *testing.T) {
			spool := &manifestSpool{options: (ManifestReaderOptions{MemoryBytes: memoryBytes}).defaults()}
			t.Cleanup(func() {
				if err := spool.close(); err != nil {
					t.Error(err)
				}
			})
			for _, value := range values {
				raw, err := manifestEncode(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := spool.append(raw); err != nil {
					t.Fatal(err)
				}
			}
			reader := sequenceReader{spool: spool}
			decoded := make([]any, 0, len(values))
			for range values {
				value, err := reader.next()
				if err != nil {
					t.Fatal(err)
				}
				decoded = append(decoded, value)
			}
			if !reflect.DeepEqual(decoded, values) {
				t.Fatal("decoded values changed across storage boundaries")
			}
		})
	}
}

type manifestCountedRange struct {
	memoryRange
	calls, maxRange, bytes int64
}

func (s *manifestCountedRange) ReadRange(ctx context.Context, at, size int64) ([]byte, error) {
	s.calls++
	s.bytes += size
	if size > s.maxRange {
		s.maxRange = size
	}
	return s.memoryRange.ReadRange(ctx, at, size)
}

func TestManifestRangeStreamCrossBufferAndCancellation(t *testing.T) {
	data := bytes.Repeat([]byte{7}, 200000)
	source := &manifestCountedRange{memoryRange: data}
	s := manifestRangeStream{ctx: context.Background(), source: source, end: uint64(len(data)), readAhead: 64 << 10}
	for _, size := range []uint64{3, 140000, 59997} {
		got, err := s.take(size)
		if err != nil || len(got) != int(size) || !bytes.Equal(got, bytes.Repeat([]byte{7}, int(size))) {
			t.Fatal(err)
		}
	}
	if source.calls != 4 || source.maxRange > 64<<10 {
		t.Fatalf("calls=%d max=%d", source.calls, source.maxRange)
	}
	if _, err := s.take(1); ErrorCategory(err) != "malformed-toc" {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s = manifestRangeStream{ctx: ctx, source: source, end: uint64(len(data)), readAhead: 64 << 10}
	if _, err := s.take(1); !errors.Is(err, context.Canceled) || source.calls != 4 {
		t.Fatalf("canceled input fetched: %v", err)
	}
}

func TestManifestWriterSnapshotsEntryAndRootExtensions(t *testing.T) {
	var out bytes.Buffer
	rootBytes := []byte{7}
	attributes := Extensions{"x-bytes": []byte{7}}
	w, err := newEntryArchiveWriter(&out, WriterOptions{FrameSize: 512, Manifest: ManifestWriterOptions{RootExtensions: Extensions{"x-bytes": rootBytes}, CompressionLevel: 1}})
	if err != nil {
		t.Fatal(err)
	}
	rootBytes[0] = 9
	if err = w.Add(context.Background(), Entry{Path: "file", Mode: 420, Attributes: attributes}, 0, nil); err != nil {
		t.Fatal(err)
	}
	mustTestValue[[]byte](t, attributes["x-bytes"])[0] = 9
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := openManifest(context.Background(), memoryRange(out.Bytes()), ManifestReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, m)

	root := m.RootExtensions()
	if mustTestValue[[]byte](t, root["x-bytes"])[0] != 7 {
		t.Fatal("root not snapshotted")
	}
	mustTestValue[[]byte](t, root["x-bytes"])[0] = 9
	if mustTestValue[[]byte](t, m.RootExtensions()["x-bytes"])[0] != 7 {
		t.Fatal("root result not owned")
	}
	entry, err := m.Entry("file")
	if err != nil || mustTestValue[[]byte](t, entry.Attributes["x-bytes"])[0] != 7 {
		t.Fatal("entry not snapshotted")
	}
	// The low-level manifest writer also snapshots caller data before flushing.
	out.Reset()
	out.Write(make([]byte, 1000))
	metadata, err := newManifestWriter(&out, 1000, ManifestWriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	extents := []Extent{{Size: 1, Source: ArchiveSource(0), EncodedSize: 1}}
	if err = metadata.AddEntry(Entry{Path: "file", Mode: 420, Extents: extents, Attributes: attributes}); err != nil {
		t.Fatal(err)
	}
	extents[0].Size = 9
	mustTestValue[[]byte](t, attributes["x-bytes"])[0] = 10
	if err = metadata.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := openManifest(context.Background(), memoryRange(out.Bytes()), ManifestReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, next)
	entry, err = next.Entry("file")
	if err != nil || entry.Extents[0].Size != 1 || mustTestValue[[]byte](t, entry.Attributes["x-bytes"])[0] != 9 {
		t.Fatal("encoded entry not snapshotted")
	}
}

func TestDirectManifestTupleIntegerBoundaries(t *testing.T) {
	link := "../target"
	for _, value := range []uint64{0, 23, 24, 255, 256, 65535, 65536, 0xffffffff, 0x100000000, math.MaxUint64} {
		for _, timestamp := range []int64{math.MinInt64, -25, -24, -1, 0, 23, 24, math.MaxInt64} {
			entry := Entry{Path: "entry", Type: 2, Mode: 420, UID: value, GID: value, MtimeNS: timestamp, Linkname: &link, Attributes: Extensions{"x-bytes": []byte{7}}}
			encoded, err := encodeEntry(entry)
			if err != nil {
				t.Fatal(err)
			}
			// fxamacker is the independent deterministic CBOR reference encoder.
			reference, err := manifestEncode(entry.value())
			if err != nil || !bytes.Equal(encoded, reference) {
				t.Fatalf("noncanonical integer %d/%d: %x != %x (%v)", value, timestamp, encoded, reference, err)
			}
			entry.Type, entry.Linkname = 0, nil
			entry.Extents = []Extent{{Size: 1, Source: ArchiveSource(value), EncodedSize: value, Offset: 1}, {Size: 1, Source: ObjectSource([32]byte{31: 7}), EncodedSize: value}}
			encoded, err = encodeEntry(entry)
			if err != nil {
				t.Fatal(err)
			}
			reference, err = manifestEncode(entry.value())
			if err != nil || !bytes.Equal(encoded, reference) {
				t.Fatalf("noncanonical extent integer %d (%v)", value, err)
			}
		}
	}
}
