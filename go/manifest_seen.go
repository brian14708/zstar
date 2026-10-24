package zstar

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
)

// Unordered physical tar needs an exact duplicate/coverage proof. Sorted tar
// uses the manifest cursor alone. Only the fallback spools entry ranks. Fixed
// 64 KiB runs and pairwise buffered merging keep memory independent of N.
func validateObservedRanks(ctx context.Context, source *manifestSpool, count uint64) (err error) {
	const runBytes = uint64(64 << 10)
	if source.size != count*8 {
		return tarMismatch("physical member coverage")
	}
	current := manifestSpool{options: source.options}
	defer func() { err = errors.Join(err, current.close()) }()
	for at := uint64(0); at < source.size; at += runBytes {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := source.read(at, min(runBytes, source.size-at))
		if err != nil {
			return err
		}
		ranks := make([]uint64, len(b)/8)
		for i := range ranks {
			ranks[i] = binary.LittleEndian.Uint64(b[i*8:])
		}
		slices.Sort(ranks)
		for i, rank := range ranks {
			binary.LittleEndian.PutUint64(b[i*8:], rank)
		}
		if err = current.append(b); err != nil {
			return err
		}
	}
	for width := runBytes; width < current.size; width *= 2 {
		next := manifestSpool{options: source.options}
		for at := uint64(0); at < current.size; at += 2 * width {
			left := rankRun{source: &current, at: at, end: min(at+width, current.size)}
			right := rankRun{source: &current, at: left.end, end: min(at+2*width, current.size)}
			if err = mergeRankRuns(ctx, &left, &right, &next); err != nil {
				return errors.Join(err, next.close())
			}
		}
		if err = current.close(); err != nil {
			return errors.Join(err, next.close())
		}
		current = next
	}
	ordered := rankRun{source: &current, end: current.size}
	for expected := range count {
		if expected%8192 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		rank, ok, err := ordered.peek()
		if err != nil {
			return err
		}
		if !ok || rank != expected {
			return tarMismatch("duplicate or missing physical member")
		}
		ordered.pop()
	}
	return nil
}

type rankRun struct {
	source  *manifestSpool
	at, end uint64
	buffer  []byte
}

func (r *rankRun) peek() (uint64, bool, error) {
	if len(r.buffer) == 0 {
		if r.at == r.end {
			return 0, false, nil
		}
		b, err := r.source.read(r.at, min(64<<10, r.end-r.at))
		if err != nil {
			return 0, false, err
		}
		r.buffer = b
		r.at += uint64(len(b))
	}
	return binary.LittleEndian.Uint64(r.buffer), true, nil
}
func (r *rankRun) pop() { r.buffer = r.buffer[8:] }

func mergeRankRuns(ctx context.Context, left, right *rankRun, destination *manifestSpool) error {
	buffer := make([]byte, 0, 64<<10)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		a, aOK, err := left.peek()
		if err != nil {
			return err
		}
		b, bOK, err := right.peek()
		if err != nil {
			return err
		}
		if !aOK && !bOK {
			return destination.append(buffer)
		}
		if aOK && (!bOK || a <= b) {
			left.pop()
		} else {
			a = b
			right.pop()
		}
		buffer = binary.LittleEndian.AppendUint64(buffer, a)
		if len(buffer) == cap(buffer) {
			if err := destination.append(buffer); err != nil {
				return err
			}
			buffer = buffer[:0]
		}
	}
}
