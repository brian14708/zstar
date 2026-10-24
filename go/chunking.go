package zstar

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math/bits"
)

// ChunkingOptions selects a boundary implementation independently of storage.
// Zero values select fixed 1 MiB chunks; FastCDC defaults to 64/256/1024 KiB.
type ChunkingOptions struct {
	FastCDC                         bool
	MinBytes, TargetBytes, MaxBytes int
}

func (o ChunkingOptions) normalized() (ChunkingOptions, error) {
	if !o.FastCDC {
		if o.TargetBytes == 0 {
			o.TargetBytes = 1 << 20
		}
		o.MinBytes, o.MaxBytes = o.TargetBytes, o.TargetBytes
		if o.TargetBytes < 1 || o.TargetBytes > 16<<20 {
			return o, errors.New("zstar: invalid fixed chunk size")
		}
	} else {
		if o.MinBytes == 0 {
			o.MinBytes = 64 << 10
		}
		if o.TargetBytes == 0 {
			o.TargetBytes = 256 << 10
		}
		if o.MaxBytes == 0 {
			o.MaxBytes = 1 << 20
		}
		if o.MinBytes < 64 || o.MinBytes > o.TargetBytes || o.TargetBytes > o.MaxBytes || o.MaxBytes > 16<<20 || o.MinBytes%2 != 0 || o.MaxBytes%2 != 0 || o.TargetBytes&(o.TargetBytes-1) != 0 {
			return o, errors.New("zstar: invalid FastCDC settings")
		}
	}
	return o, nil
}

// Chunker owns one maximum-sized window; returned chunks have compact storage.
// Input read segmentation cannot affect boundaries. Reset at each file.
type Chunker struct {
	input                io.Reader
	options              ChunkingOptions
	buffer               []byte
	filled               int
	eof                  bool
	maskSmall, maskLarge uint64
}

// NewChunker validates settings before reading the caller-owned stream.
func NewChunker(input io.Reader, options ChunkingOptions) (*Chunker, error) {
	options, err := options.normalized()
	if err != nil {
		return nil, err
	}
	if input == nil {
		return nil, errors.New("zstar: nil chunk input")
	}
	chunker := &Chunker{input: input, options: options, buffer: make([]byte, options.MaxBytes)}
	if options.FastCDC {
		bitCount := bits.TrailingZeros(uint(options.TargetBytes))
		chunker.maskSmall = fastCDCMasks[bitCount+1]
		chunker.maskLarge = fastCDCMasks[bitCount-1]
	}
	return chunker, nil
}

func (c *Chunker) cut() int {
	if !c.options.FastCDC || c.filled <= c.options.MinBytes {
		return c.filled
	}
	center, end := min(c.options.TargetBytes, c.filled)&^1, c.filled&^1
	var hash uint64
	for i := c.options.MinBytes; i < end; i += 2 {
		mask := c.maskLarge
		if i < center {
			mask = c.maskSmall
		}
		hash = (hash << 2) + (fastCDCGear[c.buffer[i]] << 1)
		if hash&(mask<<1) == 0 {
			return i
		}
		hash += fastCDCGear[c.buffer[i+1]]
		if hash&mask == 0 {
			return i + 1
		}
	}
	return c.filled
}

// Next returns io.EOF after the final chunk. A stalled custom Reader must be
// unblocked by its owner when cancelling; context is checked between reads.
func (c *Chunker) Next(ctx context.Context) ([]byte, error) {
	emptyReads := 0
	for !c.eof && c.filled < len(c.buffer) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := c.input.Read(c.buffer[c.filled:])
		if n < 0 || n > len(c.buffer)-c.filled {
			return nil, errors.New("zstar: invalid input read count")
		}
		c.filled += n
		if errors.Is(err, io.EOF) {
			c.eof = true
		} else if err != nil {
			return nil, err
		}
		if n == 0 && !c.eof {
			emptyReads++
			if emptyReads >= 100 {
				return nil, io.ErrNoProgress
			}
		} else {
			emptyReads = 0
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.filled == 0 {
		return nil, io.EOF
	}
	n := c.cut()
	chunk := append([]byte(nil), c.buffer[:n]...)
	copy(c.buffer, c.buffer[n:c.filled])
	c.filled -= n
	return chunk, nil
}

// ChunkObjectWriter allows standalone and packed storage to share chunking.
type ChunkObjectWriter interface {
	Put(context.Context, []byte) (Extent, error)
}

// ExternalizeFile returns ordered unchanged-v1 extents and enforces declared
// input size, extent and pointer limits. Empty files return no extents.
func ExternalizeFile(ctx context.Context, input io.Reader, size uint64, options ChunkingOptions, writer ChunkObjectWriter) ([]Extent, error) {
	options, err := options.normalized()
	if err != nil {
		return nil, err
	}
	if input == nil || writer == nil || size > uint64(options.MaxBytes)*65536 {
		return nil, rejected("resource-limit", "external file size or extent count")
	}
	chunker, err := NewChunker(io.LimitReader(input, int64(size)+1), options)
	if err != nil {
		return nil, err
	}
	var total uint64
	extents := []Extent{}
	for {
		raw, err := chunker.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		total += uint64(len(raw))
		if total > size || len(extents) == 65536 {
			return nil, rejected("resource-limit", "external file size or extent count")
		}
		extent, err := writer.Put(ctx, raw)
		if err != nil {
			return nil, err
		}
		address, external := extent.Source.ContentAddress()
		digest, digestErr := ParseContentAddress(address)
		if !external || digestErr != nil || digest != sha256.Sum256(raw) || extent.Offset != 0 || extent.Size != uint64(len(raw)) {
			return nil, rejected("malformed-external-content", "object writer changed chunk identity")
		}
		if err := extent.validate(0); err != nil {
			return nil, err
		}
		extents = append(extents, extent)
	}
	if total != size {
		return nil, io.ErrUnexpectedEOF
	}
	if len(extents) != 0 {
		if _, err := EncodeContentReference(extents); err != nil {
			return nil, err
		}
		if _, err := EncodeContentPointer(extents); err != nil {
			return nil, err
		}
	}
	return extents, nil
}
