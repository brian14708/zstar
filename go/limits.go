package zstar

import "github.com/klauspost/compress/zstd"

// Limits are optional caller resource budgets, independent of format validity.
// Zero disables that additional limit. Aggregate metadata, index, spool and
// concurrency budgets remain independently configurable.
type Limits struct {
	MaxFrameEncodedBytes uint64 `json:"max_frame_encoded_bytes"`
	MaxFrameDecodedBytes uint64 `json:"max_frame_decoded_bytes"`
	MaxWindowBytes       uint64 `json:"max_window_bytes"`
	MaxEntryBytes        uint64 `json:"max_entry_bytes"`
	MaxExtents           uint64 `json:"max_extents"`
	MaxPointerBytes      uint64 `json:"max_pointer_bytes"`
}

func checkLimit(value, maximum uint64, name string) error {
	if maximum != 0 && value > maximum {
		return rejected(CategoryResourceLimit, name)
	}
	return nil
}

func addressLimit(value uint64) int {
	maximum := uint64(^uint(0)>>1) - 1
	if value == 0 || value > maximum {
		value = maximum
	}
	return int(value)
}

func (l Limits) entry(e Entry, encodedBytes int) error {
	if err := checkLimit(uint64(encodedBytes), l.MaxEntryBytes, "entry metadata size"); err != nil {
		return err
	}
	if err := checkLimit(uint64(len(e.Extents)), l.MaxExtents, "entry extent count"); err != nil {
		return err
	}
	for _, extent := range e.Extents {
		if err := checkLimit(extent.Offset+extent.Size, l.MaxFrameDecodedBytes, "extent decoded selection"); err != nil {
			return err
		}
		if _, archive := extent.Source.ArchiveOffset(); archive {
			if err := checkLimit(extent.EncodedSize, l.MaxFrameEncodedBytes, "encoded frame size"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l Limits) frame(encoded []byte) error {
	if err := checkLimit(uint64(len(encoded)), l.MaxFrameEncodedBytes, "encoded frame size"); err != nil {
		return err
	}
	var header zstd.Header
	if err := header.Decode(encoded); err != nil {
		return err
	}
	if header.HasFCS {
		if err := checkLimit(header.FrameContentSize, l.MaxFrameDecodedBytes, "decoded frame size"); err != nil {
			return err
		}
	}
	window := header.WindowSize
	if header.SingleSegment {
		window = header.FrameContentSize
	}
	return checkLimit(window, l.MaxWindowBytes, "Zstandard window")
}
