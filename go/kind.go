package zstar

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
