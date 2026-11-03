package zstar

import (
	"context"
	"slices"
	"sort"
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

// readSelection keeps a request with its optional retained entry and destination.
// Nil fields select manifest lookup and an allocated result respectively.
type readSelection struct {
	ReadRequest
	entry  *Entry
	output []byte
}

func (m *Manifest) readMany(ctx context.Context, requests []ReadRequest, resolver EncodedContentResolver, verifyExternal, wantReport bool) ([][]byte, ReadReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, ReadReport{}, err
	}
	if len(requests) > MaxBatchRequests {
		return nil, ReadReport{}, rejected(CategoryResourceLimit, "batch request count")
	}
	selections := make([]readSelection, len(requests))
	for i, request := range requests {
		selections[i].ReadRequest = request
	}
	return m.readRanges(ctx, selections, resolver, verifyExternal, wantReport)
}

func (m *Manifest) readRanges(ctx context.Context, requests []readSelection, resolver EncodedContentResolver, verifyExternal, wantReport bool) ([][]byte, ReadReport, error) {
	if err := m.content.checkOpen(ctx); err != nil {
		return nil, ReadReport{}, err
	}
	jobs := make([]manifestReadJob, 0)
	byChunk := make(map[manifestChunkKey]int)
	var total uint64
	copyCount := 0
	for i, request := range requests {
		var entry Entry
		if request.entry == nil {
			var err error
			entry, err = m.entrySnapshot(request.Path)
			if err != nil {
				return nil, ReadReport{}, err
			}
		} else {
			entry = *request.entry
		}
		if entry.Type != TypeFile {
			return nil, ReadReport{}, rejected(CategoryUnsupportedOperation, "entry is not a file")
		}
		size, err := entry.indexedSize()
		if err != nil {
			return nil, ReadReport{}, err
		}
		if request.Offset > size || request.Length > size-request.Offset {
			return nil, ReadReport{}, rejected(CategoryInvalidRange, "file range")
		}
		if request.Length > m.maxRead-total {
			return nil, ReadReport{}, rejected(CategoryResourceLimit, "batch output size")
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
					return nil, ReadReport{}, rejected(CategoryUnsupportedOperation, "external resolver required")
				}
				copyCount++
				if copyCount > maxBatchCopies {
					return nil, ReadReport{}, rejected(CategoryResourceLimit, "batch extent copies")
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
	out := make([][]byte, len(requests))
	for i, request := range requests {
		out[i] = request.output
		if out[i] == nil {
			out[i] = make([]byte, int(request.Length))
		}
	}
	extents := make([]Extent, len(jobs))
	for i, job := range jobs {
		extents[i] = job.extent
	}
	if err := m.content.readMany(ctx, extents, resolver, verifyExternal, func(index int, chunk *manifestContentChunk) error {
		return copyReadJob(&jobs[index], out, chunk)
	}); err != nil {
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

func copyReadJob(job *manifestReadJob, out [][]byte, chunk *manifestContentChunk) error {
	job.verified = chunk.verified
	for _, part := range job.copies {
		if part.extentOffset > uint64(len(chunk.raw)) || part.extentSize > uint64(len(chunk.raw))-part.extentOffset {
			return rejected(CategoryInvalidRange, "decoded extent bounds")
		}
		copy(out[part.output][part.destination:part.destination+part.length], chunk.raw[part.source:part.source+part.length])
	}
	return nil
}
