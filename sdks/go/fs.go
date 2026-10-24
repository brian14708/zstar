package zstar

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"math"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	_ fs.FS          = (*Reader)(nil)
	_ fs.StatFS      = (*Reader)(nil)
	_ fs.ReadLinkFS  = (*Reader)(nil)
	_ fs.ReadDirFile = (*archiveFile)(nil)
	_ io.Seeker      = (*archiveFile)(nil)
	_ io.ReaderAt    = (*archiveFile)(nil)
	_ io.WriterTo    = (*archiveFile)(nil)
	_ io.ReaderAt    = (*File)(nil)
	_ io.WriterTo    = (*File)(nil)
)

// FileInfo returns owned file metadata using the same permission, type and
// timestamp conventions as archive/tar.Header.FileInfo. Sys returns an owned
// *Entry. Sizes that cannot be represented by fs.FileInfo return an error.
func (e Entry) FileInfo() (fs.FileInfo, error) {
	info, err := fileInfoForEntry(e)
	if err != nil {
		return nil, err
	}
	info.entry = cloneEntry(e)
	return info, nil
}

// Internal callers already own entry metadata; avoid cloning it a second time.
func fileInfoForEntry(e Entry) (entryInfo, error) {
	size, err := e.Size()
	if err != nil {
		return entryInfo{}, err
	}
	if size > math.MaxInt64 {
		return entryInfo{}, rejected("resource-limit", "file info size exceeds int64")
	}
	return entryInfo{entry: e, size: int64(size)}, nil
}

type entryInfo struct {
	entry Entry
	size  int64
}

func (i entryInfo) Name() string       { return path.Base(i.entry.Path) }
func (i entryInfo) Size() int64        { return i.size }
func (i entryInfo) ModTime() time.Time { return time.Unix(0, i.entry.MtimeNS) }
func (i entryInfo) IsDir() bool        { return i.entry.Type == TypeDirectory }
func (i entryInfo) Sys() any           { owned := cloneEntry(i.entry); return &owned }
func (i entryInfo) Mode() fs.FileMode {
	mode := fs.FileMode(i.entry.Mode & 0o777)
	if i.entry.Mode&0o4000 != 0 {
		mode |= fs.ModeSetuid
	}
	if i.entry.Mode&0o2000 != 0 {
		mode |= fs.ModeSetgid
	}
	if i.entry.Mode&0o1000 != 0 {
		mode |= fs.ModeSticky
	}
	switch i.entry.Type {
	case TypeDirectory:
		mode |= fs.ModeDir
	case TypeSymlink:
		mode |= fs.ModeSymlink
	case TypeFile:
	}
	return mode
}

func pathError(op, name string, err error) error {
	if previous, ok := errors.AsType[*fs.PathError](err); ok {
		err = previous.Err
	}
	return &fs.PathError{Op: op, Path: name, Err: err}
}

func (r *Reader) fsEntry(name string) (*Entry, error) {
	if !fs.ValidPath(name) {
		return nil, fs.ErrInvalid
	}
	if name == "." {
		if err := r.manifest.ValidateMetadata(context.Background()); err != nil {
			return nil, err
		}
		return &Entry{Path: ".", Type: TypeDirectory, Mode: 0o555}, nil
	}
	return r.Entry(name)
}

// Open opens a file or directory using fs.FS path semantics, like
// archive/zip.Reader.Open. The virtual root is ".". Symlinks are never followed.
// Close the returned handle when done; it does not close the archive or source.
// Standard file reads use a background context. For remote I/O with deadlines,
// use OpenFile and File.ReadAtContext or File.WriteToContext.
func (r *Reader) Open(name string) (fs.File, error) {
	entry, err := r.fsEntry(name)
	if err != nil {
		return nil, pathError("open", name, err)
	}
	if entry.Type == TypeSymlink {
		return nil, pathError("open", name, fs.ErrInvalid)
	}
	info, err := fileInfoForEntry(*entry)
	if err != nil {
		return nil, pathError("open", name, err)
	}
	handle := &archiveFile{reader: r, info: info, name: name}
	if entry.Type == TypeFile {
		handle.file, err = r.manifest.openEntryFile(entry, r.resolver, r.verifyExternal)
		if err != nil {
			return nil, pathError("open", name, err)
		}
	} else {
		prefix := ""
		if name != "." {
			prefix = name + "/"
		}
		handle.prefix = prefix
		r.manifest.mu.Lock()
		handle.next = sort.Search(len(r.manifest.entries), func(i int) bool {
			return r.manifest.entries[i].Path >= prefix
		})
		r.manifest.mu.Unlock()
	}
	return handle, nil
}

// Stat returns owned metadata without reading content or following symlinks.
// Unlike os.Stat, a symlink member is reported as a symlink; following links
// safely would require a traversal policy (cycles, escapes) the archive does
// not define, so callers resolve targets themselves with ReadLink.
// Missing names and invalid paths return *fs.PathError.
func (r *Reader) Stat(name string) (fs.FileInfo, error) {
	entry, err := r.fsEntry(name)
	if err != nil {
		return nil, pathError("stat", name, err)
	}
	info, err := fileInfoForEntry(*entry)
	if err != nil {
		return nil, pathError("stat", name, err)
	}
	return info, nil
}

// Lstat returns owned metadata for a member without following symlinks.
// It implements fs.ReadLinkFS together with ReadLink.
func (r *Reader) Lstat(name string) (fs.FileInfo, error) {
	info, err := r.Stat(name)
	if err != nil {
		return nil, pathError("lstat", name, err)
	}
	return info, nil
}

type archiveFile struct {
	mu     sync.Mutex
	reader *Reader
	file   *File
	info   fs.FileInfo
	name   string
	offset int64
	prefix string
	next   int
	closed bool
}

func (f *archiveFile) checkOpen() error {
	if f.closed {
		return fs.ErrClosed
	}
	f.reader.manifest.mu.Lock()
	defer f.reader.manifest.mu.Unlock()
	if f.reader.manifest.closed {
		return fs.ErrClosed
	}
	return nil
}

func (f *archiveFile) Stat() (fs.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkOpen(); err != nil {
		return nil, pathError("stat", f.name, err)
	}
	return f.info, nil
}

func (f *archiveFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *archiveFile) Read(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkOpen(); err != nil {
		return 0, pathError("read", f.name, err)
	}
	if f.file == nil {
		return 0, pathError("read", f.name, fs.ErrInvalid)
	}
	n, err := f.file.ReadAt(b, f.offset)
	f.offset += int64(n)
	if err != nil && !errors.Is(err, io.EOF) {
		err = pathError("read", f.name, err)
	}
	return n, err
}

// ReadAt implements io.ReaderAt for regular files; it does not move the
// sequential offset used by Read and Seek.
func (f *archiveFile) ReadAt(b []byte, offset int64) (int, error) {
	f.mu.Lock()
	err := f.checkOpen()
	file := f.file
	f.mu.Unlock()
	if err != nil {
		return 0, pathError("read", f.name, err)
	}
	if file == nil {
		return 0, pathError("read", f.name, fs.ErrInvalid)
	}
	n, err := file.ReadAt(b, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		err = pathError("read", f.name, err)
	}
	return n, err
}

// Seek implements io.Seeker for regular files.
func (f *archiveFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkOpen(); err != nil {
		return 0, pathError("seek", f.name, err)
	}
	if f.file == nil {
		return 0, pathError("seek", f.name, fs.ErrInvalid)
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = f.offset
	case io.SeekEnd:
		base = f.info.Size()
	default:
		return 0, pathError("seek", f.name, fs.ErrInvalid)
	}
	if offset > math.MaxInt64-base || base+offset < 0 {
		return 0, pathError("seek", f.name, fs.ErrInvalid)
	}
	f.offset = base + offset
	return f.offset, nil
}

// WriteTo streams the remainder of the file from the current offset, letting
// io.Copy avoid intermediate buffers.
func (f *archiveFile) WriteTo(w io.Writer) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkOpen(); err != nil {
		return 0, pathError("read", f.name, err)
	}
	if f.file == nil {
		return 0, pathError("read", f.name, fs.ErrInvalid)
	}
	var n int64
	var err error
	if f.offset == 0 {
		n, err = f.file.WriteTo(w)
	} else if size := f.info.Size(); f.offset < size {
		n, err = io.Copy(w, io.NewSectionReader(f.file, f.offset, size-f.offset))
	}
	f.offset += n
	if err != nil {
		err = pathError("read", f.name, err)
	}
	return n, err
}

func (f *archiveFile) ReadDir(n int) ([]fs.DirEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkOpen(); err != nil {
		return nil, pathError("readdir", f.name, err)
	}
	if f.file != nil {
		return nil, pathError("readdir", f.name, fs.ErrInvalid)
	}
	m := f.reader.manifest
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, pathError("readdir", f.name, fs.ErrClosed)
	}
	entries := make([]fs.DirEntry, 0)
	for f.next < len(m.entries) && (n <= 0 || len(entries) < n) {
		entry := m.entries[f.next]
		if !strings.HasPrefix(entry.Path, f.prefix) {
			break
		}
		f.next++
		if slash := strings.IndexByte(entry.Path[len(f.prefix):], '/'); slash >= 0 {
			// Descendants occupy one contiguous lexicographic range. The byte
			// after '/' is '0', giving the first key beyond this subtree.
			end := entry.Path[:len(f.prefix)+slash] + "0"
			f.next += sort.Search(len(m.entries)-f.next, func(i int) bool {
				return m.entries[f.next+i].Path >= end
			})
			continue
		}
		info, err := entry.FileInfo()
		if err != nil {
			return entries, pathError("readdir", f.name, err)
		}
		entries = append(entries, fs.FileInfoToDirEntry(info))
	}
	if n > 0 && len(entries) == 0 {
		return entries, io.EOF
	}
	return entries, nil
}
