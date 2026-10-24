package zstar

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Fuzzing stays in memory with strict aggregate/read budgets and no resolver.
func FuzzIndexedArchive(f *testing.F) {
	for _, name := range []string{"empty", "inline", "thin", "invalid/data-checksum", "invalid/tar-header"} {
		data, err := os.ReadFile(filepath.Join("../testdata/entry-manifest", name+".zstar"))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data, uint64(0), uint64(6))
	}
	f.Fuzz(func(t *testing.T, data []byte, offset, length uint64) {
		if len(data) > 1<<20 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		reader, err := OpenWithOptions(ctx, memoryRange(data), ReaderOptions{Manifest: ManifestOptions{MaxEncodedBytes: 64 << 10, MaxDecodedBytes: 64 << 10, MaxReadBytes: 64 << 10, ConcurrentChunks: 1, DisableDisk: true}, DisableContentCache: true})
		if err != nil {
			return
		}
		defer testClose(t, reader)
		cursor := reader.Cursor()
		defer cursor.Close()
		entries, err := cursor.Next(8)
		if err == nil {
			for _, entry := range entries {
				if entry.Type == TypeFile {
					_, _ = reader.Read(ctx, entry.Path, offset, length) //nolint:errcheck // Malformed inputs may fail; this campaign checks bounded execution and panics.
					break
				}
			}
		}
		_, _ = reader.Verify(ctx, false) //nolint:errcheck // Malformed inputs may fail; this campaign checks bounded execution and panics.
	})
}

func FuzzCanonicalMetadata(f *testing.F) {
	root, err := manifestEncode(map[string]any{"version": uint64(1), "required_features": []string{}})
	if err != nil {
		f.Fatal(err)
	}
	entry, err := encodeEntry(Entry{Path: "empty", Type: TypeFile, Mode: 0o644})
	if err != nil {
		f.Fatal(err)
	}
	for _, raw := range [][]byte{root, entry, append(root, entry...), {0x9f, 0xff}, {0xa2, 0x01, 0x00, 0x01, 0x00}} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64<<10 {
			return
		}
		reader := sequenceReader{spool: &manifestSpool{memory: data, size: uint64(len(data))}}
		for {
			at := reader.at
			if _, err := reader.next(); err != nil {
				return
			}
			if reader.at <= at {
				t.Fatal("successful CBOR item did not advance the sequence")
			}
		}
	})
}

func TestMetadataBudgetRejectsBeforeMetadataFetch(t *testing.T) {
	data, err := os.ReadFile("../testdata/entry-manifest/inline.zstar")
	if err != nil {
		t.Fatal(err)
	}
	source := &countedSource{data: data}
	if _, err := OpenWithOptions(context.Background(), source, ReaderOptions{Manifest: ManifestOptions{MaxDecodedBytes: 1}}); ErrorCategory(err) != "resource-limit" {
		t.Fatal(err)
	}
	if source.reads != 1 {
		t.Fatalf("budget rejection fetched metadata: %d", source.reads)
	}
}

func FuzzDecodeDiskChunk(f *testing.F) {
	valid := encodeDiskChunk(CachedChunk{key: DecodedCacheKey{Kind: 1, Identity: [32]byte{1}, Offset: 7}, raw: []byte("payload"), verified: true})
	f.Add(valid)
	f.Add(valid[:diskCacheHeaderBytes])
	f.Add([]byte(diskCacheMagic))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		value, err := decodeDiskChunk(data)
		if err != nil {
			return
		}
		// Accepted inputs must round-trip exactly and never alias beyond bounds.
		if again := encodeDiskChunk(value); !bytes.Equal(again, data) {
			t.Fatalf("accepted non-canonical chunk")
		}
		if cap(value.raw) != len(value.raw) {
			t.Fatal("raw capacity exposes digest bytes")
		}
	})
}
