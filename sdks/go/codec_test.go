package zstar

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestBoundedSingleFrameDecodeAndChecksums(t *testing.T) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderCRC(true))
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, encoder)
	raw := bytes.Repeat([]byte("hello"), 1000)
	encoded := encoder.EncodeAll(raw, nil)
	decoded, err := decodeBoundedFrame(encoded, 0, len(raw), 17<<20)
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("declared-size frame: %v", err)
	}
	if cap(decoded) > len(raw)+1 {
		t.Fatal("known-size frame retained grown allocation")
	}
	for _, test := range []struct {
		data                                 []byte
		expected, decodedLimit, encodedLimit int
	}{
		{encoded, 0, len(raw) - 1, 17 << 20},
		{encoded, len(raw) - 1, len(raw), 17 << 20},
		{encoded, 0, len(raw), len(encoded) - 1},
		{append(bytes.Clone(encoded), encoded...), 0, 2 * len(raw), 17 << 20},
		{append(bytes.Clone(encoded), 0), 0, len(raw), 17 << 20},
		{encoded[:len(encoded)-1], 0, len(raw), 17 << 20},
	} {
		if _, err := decodeBoundedFrame(test.data, test.expected, test.decodedLimit, test.encodedLimit); err == nil {
			t.Fatal("invalid frame or budget accepted")
		}
	}
	corrupt := bytes.Clone(encoded)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := decodeBoundedFrame(corrupt, 0, len(raw), 17<<20); err == nil {
		t.Fatal("checksum corruption accepted")
	}
}

func TestDefaultReaderRejectsShortSources(t *testing.T) {
	for _, data := range [][]byte{nil, make([]byte, 79)} {
		if _, err := Open(context.Background(), memoryRange(data)); ErrorCategory(err) != "not-zstar" {
			t.Fatal(err)
		}
	}
	source := ReaderAtSource{Reader: bytes.NewReader([]byte("hello")), Length: 5}
	if _, err := source.ReadRange(context.Background(), -1, 1); err == nil {
		t.Fatal("negative source range")
	}
	if _, err := source.ReadRange(context.Background(), 4, 2); err == nil {
		t.Fatal("oversized source range")
	}
	source.Length = 6
	if _, err := source.ReadRange(context.Background(), 0, 6); !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("short ReaderAt source accepted")
	}
}
