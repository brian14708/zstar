package zstar

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestValidateArchiveRejectsPointerBeyondAddressSpace(t *testing.T) {
	for _, size := range []uint64{1 << 40, uint64(^uint(0)>>1) + 1} {
		t.Run(strconv.FormatUint(size, 10), func(t *testing.T) { testValidateArchiveRejectsHugePointer(t, size) })
	}
}

func testValidateArchiveRejectsHugePointer(t *testing.T, size uint64) {
	t.Helper()
	ctx := context.Background()
	header, err := memberHeader(Header{Path: "pointer", Type: TypeFile, Mode: 420, Size: size})
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderCRC(true))
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, encoder)
	var output bytes.Buffer
	output.Write(encoder.EncodeAll(header, nil))
	writer, err := newManifestWriter(&output, uint64(output.Len()), ManifestWriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entry := Entry{Path: "pointer", Mode: 420, Extents: []Extent{{
		Size: 1, Source: mustObjectSource(t, "sha256:"+strings.Repeat("0", 64)), EncodedSize: 1,
	}}}
	if err = writer.AddEntry(entry); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, err := openManifest(ctx, memoryRange(output.Bytes()), ManifestReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, manifest)
	want := CategoryMalformedTar
	if size > uint64(^uint(0)>>1) {
		want = CategoryResourceLimit
	}
	if err = manifest.ValidateArchive(ctx); ErrorCategory(err) != want {
		t.Fatalf("oversized pointer: %v", err)
	}
}

func tarChecksum(raw []byte) {
	copy(raw[148:156], []byte("        "))
	sum := uint64(0)
	for _, b := range raw {
		sum += uint64(b)
	}
	copy(raw[148:156], fmt.Appendf(nil, "%07o\x00", sum))
}

func TestStrictTarNumericGrammars(t *testing.T) {
	for _, raw := range [][]byte{[]byte("+1"), []byte("-1"), []byte("8"), []byte("0\x001"), []byte("\x001"), {128}} {
		if _, err := tarOctal(raw); ErrorCategory(err) != "malformed-tar" {
			t.Fatalf("accepted %q: %v", raw, err)
		}
	}
	if n, err := tarOctal([]byte(" 0017\x00 ")); n != 15 || err != nil {
		t.Fatalf("octal: %d %v", n, err)
	}
	for _, text := range []string{"+1", "--1", "-+1", "1.", ".1", "1.0000000001", "9223372036.854775808", "-9223372036.854775809", "18446744073709551616"} {
		if _, err := paxMtime(text); ErrorCategory(err) != "malformed-tar" {
			t.Fatalf("accepted mtime %q: %v", text, err)
		}
	}
	for text, want := range map[string]int64{"-0.000000001": -1, "1.0000000000": 1_000_000_000, "9223372036.854775807": math.MaxInt64, "-9223372036.854775808": math.MinInt64} {
		if got, err := paxMtime(text); err != nil || got != want {
			t.Fatalf("mtime %s: %d %v", text, got, err)
		}
	}
	for _, text := range []string{"1 a=\n", "0 a=b\n", "999999999999999999999999 a=b\n", "12 uid=0\n", "8 uid=\n", "9 bad=x\n"} {
		if _, err := parsePAX([]byte(text)); err == nil {
			t.Fatalf("accepted bad PAX %q", text)
		}
	}
}

// Parse physical header sequences independently of manifest offsets.
func parseTestHeader(raw []byte) (effectiveManifestHeader, error) {
	c := manifestTarCursor{input: manifestRangeStream{ctx: context.Background()}, raw: raw[512:]}
	return c.header(raw[:512])
}

func TestPAXHeadersValidateEvenOverriddenFields(t *testing.T) {
	headers, err := memberHeader(Header{Path: "long-" + strings.Repeat("a", 110), Type: TypeFile, UID: 1 << 40, GID: 1 << 41, MtimeNS: -1, Mode: 420, Size: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseTestHeader(headers); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct {
		at    int
		value byte
	}{{265, 'x'}, {297, 'x'}, {500, 1}, {257, 'X'}, {108, 128}, {len(headers) - 512 + 108, 128}} {
		raw := bytes.Clone(headers)
		raw[mutation.at] = mutation.value
		at := mutation.at / 512 * 512
		tarChecksum(raw[at : at+512])
		if _, err := parseTestHeader(raw); ErrorCategory(err) != "malformed-tar" {
			t.Fatalf("field %d: %v", mutation.at, err)
		}
	}
	local, err := parseTarHeader(headers[:512])
	if err != nil {
		t.Fatal(err)
	}
	raw := bytes.Clone(headers)
	raw[512+local.size] = 1
	if _, err := parseTestHeader(raw); ErrorCategory(err) != "malformed-tar" {
		t.Fatal(err)
	}
}

func TestUstarPrefixAndExactDirectoryName(t *testing.T) {
	raw, err := memberHeader(Header{Path: "dir/file", Type: TypeFile, Mode: 420})
	if err != nil {
		t.Fatal(err)
	}
	clear(raw[:100])
	copy(raw[:100], "file")
	copy(raw[345:500], "dir")
	tarChecksum(raw)
	header, err := parseTestHeader(raw)
	if err != nil || header.entry.Path != "dir/file" {
		t.Fatal(err)
	}
	raw, err = memberHeader(Header{Path: "dir", Type: TypeDirectory, Mode: 493})
	if err != nil {
		t.Fatal(err)
	}
	raw[3] = 0
	tarChecksum(raw)
	if _, err := parseTestHeader(raw); ErrorCategory(err) != "malformed-tar" {
		t.Fatal(err)
	}
}
