package zstar

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
)

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
	headers, err := memberHeader(Header{Path: "long-" + strings.Repeat("a", 110), Kind: "file", UID: 1 << 40, GID: 1 << 41, MtimeNS: -1, Mode: 420, Size: 3})
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
	raw, err := memberHeader(Header{Path: "dir/file", Kind: "file", Mode: 420})
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
	raw, err = memberHeader(Header{Path: "dir", Kind: "directory", Mode: 493})
	if err != nil {
		t.Fatal(err)
	}
	raw[3] = 0
	tarChecksum(raw)
	if _, err := parseTestHeader(raw); ErrorCategory(err) != "malformed-tar" {
		t.Fatal(err)
	}
}
