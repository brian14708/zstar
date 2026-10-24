package zstar

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type frameDecoder struct{ decoder *zstd.Decoder }

func (d *frameDecoder) close() {
	if d.decoder != nil {
		d.decoder.Close()
		d.decoder = nil
	}
}

// One idle workspace per reader; concurrent workers are already bounded by
// ConcurrentChunks. Excess workspaces are closed rather than retained globally.
type frameDecoderPool struct {
	mu     sync.Mutex
	idle   *frameDecoder
	closed bool
}

func (p *frameDecoderPool) decode(encoded []byte, expected int) ([]byte, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("zstar: closed decoder pool")
	}
	d := p.idle
	p.idle = nil
	p.mu.Unlock()
	if d == nil {
		d = &frameDecoder{}
	}
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.closed || p.idle != nil {
			d.close()
		} else {
			p.idle = d
		}
	}()
	return d.decode(encoded, expected, maxFrameSize, 17<<20)
}

func (p *frameDecoderPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.idle != nil {
		p.idle.close()
		p.idle = nil
	}
}

func decodeBoundedFrame(encoded []byte, expected, decodedLimit, encodedLimit int) ([]byte, error) {
	d := frameDecoder{}
	defer d.close()
	return d.decode(encoded, expected, decodedLimit, encodedLimit)
}

func (d *frameDecoder) decode(encoded []byte, expected, decodedLimit, encodedLimit int) ([]byte, error) {
	if expected < 0 || expected > decodedLimit || len(encoded) > encodedLimit {
		return nil, errors.New("zstar: frame size limits")
	}
	var header zstd.Header
	if err := header.Decode(encoded); err != nil {
		return nil, err
	}
	if header.Skippable || header.DictionaryID != 0 || header.WindowSize > maxFrameSize || header.SingleSegment && header.FrameContentSize > maxFrameSize {
		return nil, errors.New("zstar: unsupported frame or window limit")
	}
	// Walk block lengths so concatenated frames and trailing bytes cannot pass
	// merely because their combined decoded length matches the declared size.
	pos := header.HeaderSize
	for {
		if len(encoded)-pos < 3 {
			return nil, io.ErrUnexpectedEOF
		}
		block := uint32(encoded[pos]) | uint32(encoded[pos+1])<<8 | uint32(encoded[pos+2])<<16
		pos += 3
		size := int(block >> 3)
		kind := (block >> 1) & 3
		if kind == 3 {
			return nil, errors.New("zstar: reserved block type")
		}
		if kind == 1 {
			size = 1
		}
		if size > len(encoded)-pos {
			return nil, io.ErrUnexpectedEOF
		}
		pos += size
		if block&1 != 0 {
			break
		}
	}
	if header.HasCheckSum {
		pos += 4
	}
	if pos != len(encoded) {
		return nil, errors.New("zstar: expected exactly one Zstandard frame")
	}
	// A declared content size is bounded before allocation and checked again
	// by the decoder. Reuse it to reserve once for unknown-size callers.
	if expected == 0 && header.HasFCS {
		if header.FrameContentSize > uint64(decodedLimit) {
			return nil, errors.New("zstar: decoded output limit")
		}
		if header.FrameContentSize != 0 {
			expected = int(header.FrameContentSize)
		}
	}
	if d.decoder == nil {
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(maxFrameSize), zstd.WithDecoderMaxMemory(maxFrameSize))
		if err != nil {
			return nil, err
		}
		d.decoder = decoder
	}
	decoder := d.decoder
	if err := decoder.Reset(bytes.NewReader(encoded)); err != nil {
		return nil, err
	}
	defer func() { _ = decoder.Reset(nil) }() //nolint:errcheck // Nil Reset releases input references on a live decoder.

	if expected == 0 {
		raw, readErr := io.ReadAll(io.LimitReader(decoder, int64(decodedLimit)+1))
		if readErr != nil {
			return nil, readErr
		}
		if len(raw) > decodedLimit {
			return nil, errors.New("zstar: decoded output limit")
		}
		return raw, nil
	}
	// Reserve once rather than repeatedly growing io.ReadAll's buffer. Keep
	// one spare byte to detect oversize output and read through EOF/checksum.
	raw := make([]byte, expected+1)
	n := 0
	for {
		count, readErr := decoder.Read(raw[n:])
		n += count
		if n > expected {
			return nil, errors.New("zstar: decoded output limit")
		}
		if errors.Is(readErr, io.EOF) {
			if n != expected {
				return nil, errors.New("zstar: decoded size mismatch")
			}
			return raw[:n], nil
		}
		if readErr != nil {
			return nil, readErr
		}
	}
}
