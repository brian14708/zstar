package zstar

import (
	"context"
	"errors"
	"io"
)

// ReaderAtSource adapts a local file or any io.ReaderAt. It never closes Reader.
// Length is the immutable encoded archive size, for example from File.Stat.
type ReaderAtSource struct {
	Reader io.ReaderAt
	Length int64
}

// Size returns the immutable encoded source length and checks ctx.
func (s ReaderAtSource) Size(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.Reader == nil || s.Length < 0 {
		return 0, errors.New("zstar: invalid ReaderAt source")
	}
	return s.Length, nil
}

// ReadRange returns an owned buffer for an exact source range. ctx is
// checked before and after Reader.ReadAt; it cannot interrupt a blocking
// caller-owned ReaderAt. Reader must support concurrent calls.
func (s ReaderAtSource) ReadRange(ctx context.Context, offset, length int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Reader == nil || offset < 0 || length < 0 || offset > s.Length || length > s.Length-offset {
		return nil, errors.New("zstar: source range out of bounds")
	}
	if length == 0 {
		return []byte{}, nil
	}
	if uint64(length) > uint64(^uint(0)>>1) {
		return nil, errors.New("zstar: source range exceeds address space")
	}
	data := make([]byte, int(length))
	n, err := s.Reader.ReadAt(data, offset)
	if err != nil && (!errors.Is(err, io.EOF) || n != len(data)) {
		return nil, err
	}
	if n != len(data) {
		return nil, io.ErrUnexpectedEOF
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}
