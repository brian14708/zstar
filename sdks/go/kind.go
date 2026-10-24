package zstar

// Kind identifies the member kind supplied to a Writer. The empty kind means
// KindFile, matching the zero-value regular-file entry type.
type Kind string

const (
	// KindFile denotes a regular file, including an external content reference.
	KindFile Kind = "file"
	// KindDirectory denotes a directory with no body.
	KindDirectory Kind = "directory"
	// KindSymlink denotes a symbolic link whose target is preserved verbatim.
	KindSymlink Kind = "symlink"
)

// EntryType identifies the effective tar member type stored in the manifest.
type EntryType uint64

const (
	// TypeFile denotes a regular file.
	TypeFile EntryType = 0
	// TypeSymlink denotes a symbolic link.
	TypeSymlink EntryType = 2
	// TypeDirectory denotes a directory.
	TypeDirectory EntryType = 5
)
