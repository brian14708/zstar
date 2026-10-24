package zstar

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func spoolOf(t *testing.T, b []byte) *manifestSpool {
	t.Helper()
	s := &manifestSpool{options: ManifestOptions{}.defaults()}
	if err := s.append(b); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.close() }) //nolint:errcheck // Test cleanup.
	return s
}

func TestSequenceRejectsOversizedItemsBeforeAllocating(t *testing.T) {
	// Byte string claiming 4 GiB, then truncated; must be a limit error, not
	// a "truncated" error and not an allocation.
	giant := []byte{0x5b, 0, 0, 0, 1, 0, 0, 0, 0}
	r := &sequenceReader{spool: spoolOf(t, giant), limit: manifestEntryMax}
	if _, err := r.next(); ErrorCategory(err) != CategoryResourceLimit {
		t.Fatalf("giant bstr: %v", err)
	}
	// Claimed length within the spool but above the cap.
	big := append([]byte{0x5a, 0, 0x90, 0, 0}, make([]byte, 0x900000)...)
	r = &sequenceReader{spool: spoolOf(t, big), limit: manifestEntryMax}
	if _, err := r.next(); ErrorCategory(err) != CategoryResourceLimit {
		t.Fatalf("oversized bstr: %v", err)
	}
	// Huge array claim.
	r = &sequenceReader{spool: spoolOf(t, append([]byte{0x9a, 0x7f, 0xff, 0xff, 0xff}, make([]byte, 64)...)), limit: 32}
	if _, err := r.next(); ErrorCategory(err) != CategoryResourceLimit {
		t.Fatalf("giant array: %v", err)
	}
}

func buildEntriesArchive(t *testing.T, n int, opts ManifestOptions) []byte {
	t.Helper()
	var out bytes.Buffer
	out.WriteByte(0)
	w, err := NewManifestWriter(&out, 1, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		e := Entry{Path: "d" + string(rune('a'+i/26)) + string(rune('a'+i%26)), Type: TypeDirectory, Mode: 0o755}
		if err := w.AddEntry(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestManifestEntryBudgets(t *testing.T) {
	data := buildEntriesArchive(t, 50, ManifestOptions{})
	for name, o := range map[string]ManifestOptions{
		"count": {MaxEntries: 10},
		"bytes": {MaxRetainedEntryBytes: 1000},
	} {
		_, err := OpenManifest(context.Background(), memoryRange(data), o)
		if ErrorCategory(err) != CategoryResourceLimit {
			t.Errorf("%s: %v", name, err)
		}
	}
	m, err := OpenManifest(context.Background(), memoryRange(data), ManifestOptions{MaxEntries: 50})
	if err != nil {
		t.Fatal(err)
	}
	testClose(t, m)
	_, err = OpenManifest(context.Background(), memoryRange(data), ManifestOptions{MaxEntries: 1})
	if _, ok := errors.AsType[*FormatError](err); !ok {
		t.Fatal("not a FormatError")
	}
}
