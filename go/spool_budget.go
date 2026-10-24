package zstar

import "sync/atomic"

// SpoolBudget bounds retained logical bytes across memory and spill stores.
// Sharing its pointer across options shares the quota. It must not be copied.
// Filesystem overhead, migration copies and decoded caches are separate.
type SpoolBudget struct {
	limit uint64
	used  atomic.Uint64
}

// NewSpoolBudget creates a quota; a zero limit disallows retaining spool bytes.
func NewSpoolBudget(limit uint64) *SpoolBudget { return &SpoolBudget{limit: limit} }

// Limit returns the configured logical spool quota in bytes.
func (b *SpoolBudget) Limit() uint64 { return b.limit }

// Used returns the current retained logical spool charge in bytes.
func (b *SpoolBudget) Used() uint64 { return b.used.Load() }

func (b *SpoolBudget) reserve(size uint64) error {
	if size == 0 {
		return nil
	}
	for {
		used := b.used.Load()
		if used > b.limit || size > b.limit-used {
			return rejected("resource-limit", "aggregate spool storage")
		}
		if b.used.CompareAndSwap(used, used+size) {
			return nil
		}
	}
}

func (b *SpoolBudget) release(size uint64) {
	if size != 0 {
		b.used.Add(^(size - 1))
	}
}
