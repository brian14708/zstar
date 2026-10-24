package zstar

import (
	"context"
	"io/fs"
	"slices"
	"sort"
	"sync"
)

// MaxBatchRequests is the local request-count limit for Reader.ReadMany.
const (
	MaxBatchRequests = 4096
	maxBatchCopies   = 65536
)

// ReadRequest selects a logical member range without following symlinks.
type ReadRequest struct {
	Path           string
	Offset, Length uint64
}

// ReadReport describes selected content only. Successful reads always
// authenticate metadata and check archive data-frame checksums; they never
// establish that unread tar headers, pointer text or end markers are valid.
type ReadReport struct {
	MetadataAuthenticated        bool     `json:"metadata_authenticated"`
	FullArchiveValidated         bool     `json:"full_archive_validated"`
	DataFramesChecked            []uint64 `json:"data_frames_checked"`
	ExternalObjectsSHA256Checked []string `json:"external_objects_sha256_checked"`
	ExternalObjectsUnchecked     []string `json:"external_objects_unchecked"`
}
type manifestReadCopy struct {
	output                   int
	source, destination      uint64
	length                   uint64
	extentOffset, extentSize uint64
}

type manifestReadJob struct {
	extent   Extent
	copies   []manifestReadCopy
	verified bool
}

// ReadMany validates every range and the aggregate output/copy budgets before
// content I/O. Frames/objects are grouped once per batch, including with caching
// disabled. Outputs are compact, independent owned buffers in caller order.
// Sources and resolvers must support concurrent calls. Cancellation joins batch
// workers; uninterruptible source callbacks retain their bounded work slots.
func (m *Manifest) ReadMany(ctx context.Context, requests []ReadRequest, resolver EncodedContentResolver, verifyExternal bool) ([][]byte, error) {
	out, _, err := m.readMany(ctx, requests, resolver, verifyExternal, false)
	return out, err
}

// ReadManyWithReport reads a bounded batch and reports which selected chunks
// were checked. Failure returns no results and joins the batch workers.
func (m *Manifest) ReadManyWithReport(ctx context.Context, requests []ReadRequest, resolver EncodedContentResolver, verifyExternal bool) ([][]byte, ReadReport, error) {
	return m.readMany(ctx, requests, resolver, verifyExternal, true)
}

// ReadWithReport reads a bounded member range and reports selected-content
// checks. The returned bytes are owned by the caller.
func (m *Manifest) ReadWithReport(ctx context.Context, path string, offset, size uint64, resolver EncodedContentResolver, verifyExternal bool) ([]byte, ReadReport, error) {
	out, report, err := m.readMany(ctx, []ReadRequest{{path, offset, size}}, resolver, verifyExternal, true)
	if err != nil {
		return nil, ReadReport{}, err
	}
	return out[0], report, nil
}

func (m *Manifest) readMany(ctx context.Context, requests []ReadRequest, resolver EncodedContentResolver, verifyExternal, wantReport bool) ([][]byte, ReadReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, ReadReport{}, err
	}
	if len(requests) > MaxBatchRequests {
		return nil, ReadReport{}, rejected("resource-limit", "batch request count")
	}
	// Check even empty batches against the reader's lifecycle.
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return nil, ReadReport{}, categorized("unsupported-operation", fs.ErrClosed)
	}
	jobs := make([]manifestReadJob, 0)
	byChunk := make(map[manifestChunkKey]int)
	var total uint64
	copyCount := 0
	for i, request := range requests {
		entry, err := m.entrySnapshot(request.Path)
		if err != nil {
			return nil, ReadReport{}, err
		}
		if entry.Type != TypeFile {
			return nil, ReadReport{}, rejected("unsupported-operation", "entry is not a file")
		}
		size, err := entry.indexedSize()
		if err != nil {
			return nil, ReadReport{}, err
		}
		if request.Offset > size || request.Length > size-request.Offset {
			return nil, ReadReport{}, rejected("invalid-range", "file range")
		}
		if request.Length > m.maxRead-total {
			return nil, ReadReport{}, rejected("resource-limit", "batch output size")
		}
		total += request.Length
		if request.Length == 0 {
			continue
		}
		end := request.Offset + request.Length
		var position uint64
		first := 0
		if len(entry.extentEnds) != 0 {
			first = sort.Search(len(entry.extentEnds), func(i int) bool { return entry.extentEnds[i] > request.Offset })
			if first != 0 {
				position = entry.extentEnds[first-1]
			}
		}
		for _, extent := range entry.Extents[first:] {
			next := position + extent.Size
			if position < end && next > request.Offset {
				if entry.External() && resolver == nil {
					return nil, ReadReport{}, rejected("unsupported-operation", "external resolver required")
				}
				copyCount++
				if copyCount > maxBatchCopies {
					return nil, ReadReport{}, rejected("resource-limit", "batch extent copies")
				}
				key := chunkKey(extent)
				job, exists := byChunk[key]
				if !exists {
					job = len(jobs)
					byChunk[key] = job
					jobs = append(jobs, manifestReadJob{extent: extent})
				}
				a, b := max(position, request.Offset), min(next, end)
				jobs[job].copies = append(jobs[job].copies, manifestReadCopy{output: i, source: extent.Offset + a - position, destination: a - request.Offset, length: b - a, extentOffset: extent.Offset, extentSize: extent.Size})
			}
			position = next
			if position >= end {
				break
			}
		}
	}
	ctx, err := m.prefetch(ctx, jobs, resolver)
	if err != nil {
		return nil, ReadReport{}, err
	}
	out := make([][]byte, len(requests))
	for i, request := range requests {
		out[i] = make([]byte, int(request.Length))
	}
	var resultError error
	if len(jobs) == 1 {
		// Grouped requests often select one physical chunk. Loading it directly
		// avoids a batch worker; the content loader still coalesces misses and
		// retains its work permit until an uninterruptible callback returns.
		resultError = m.readJob(ctx, &jobs[0], out, resolver, verifyExternal)
	} else if len(jobs) > 1 {
		work, cancel := context.WithCancel(ctx)
		defer cancel()
		var wg sync.WaitGroup
		var errorOnce sync.Once
		queue := make(chan int)
		workers := min(cap(m.content.slots), len(jobs))
		wg.Add(workers)
		for range workers {
			go func() {
				defer wg.Done()
				for index := range queue {
					if err := m.readJob(work, &jobs[index], out, resolver, verifyExternal); err != nil {
						errorOnce.Do(func() { resultError = err; cancel() })
						return
					}
				}
			}()
		}
	send:
		for i := range jobs {
			select {
			case queue <- i:
			case <-work.Done():
				break send
			}
		}
		close(queue)
		wg.Wait()
	}
	if resultError != nil {
		return nil, ReadReport{}, resultError
	}
	if err := ctx.Err(); err != nil {
		return nil, ReadReport{}, err
	}
	var report ReadReport
	if wantReport {
		report.MetadataAuthenticated = true
		report.DataFramesChecked = []uint64{}
		report.ExternalObjectsSHA256Checked, report.ExternalObjectsUnchecked = []string{}, []string{}
		for _, job := range jobs {
			if source, archive := job.extent.Source.ArchiveOffset(); archive {
				report.DataFramesChecked = append(report.DataFramesChecked, source)
			} else {
				source := job.extent.Source.address
				if job.verified {
					report.ExternalObjectsSHA256Checked = append(report.ExternalObjectsSHA256Checked, source)
				} else {
					report.ExternalObjectsUnchecked = append(report.ExternalObjectsUnchecked, source)
				}
			}
		}
		slices.Sort(report.DataFramesChecked)
		sort.Strings(report.ExternalObjectsSHA256Checked)
		sort.Strings(report.ExternalObjectsUnchecked)
	}
	return out, report, nil
}

func (m *Manifest) readJob(ctx context.Context, job *manifestReadJob, out [][]byte, resolver EncodedContentResolver, verifyExternal bool) error {
	chunk, err := m.content.get(ctx, job.extent, resolver, verifyExternal)
	if err != nil {
		return err
	}
	job.verified = chunk.verified
	for _, part := range job.copies {
		if part.extentOffset > uint64(len(chunk.raw)) || part.extentSize > uint64(len(chunk.raw))-part.extentOffset {
			return rejected("invalid-range", "decoded extent bounds")
		}
		copy(out[part.output][part.destination:part.destination+part.length], chunk.raw[part.source:part.source+part.length])
	}
	return nil
}
