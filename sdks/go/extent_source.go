package zstar

import (
	"encoding/hex"
	"strconv"
)

// ExtentSource identifies an archive frame or a SHA-256 addressed object.
// Its zero value identifies archive offset zero. Values are comparable and
// immutable. Use ArchiveSource, ObjectSource, or ParseObjectSource to create one.
type ExtentSource struct {
	offset  uint64
	address string
}

// ArchiveSource identifies an absolute encoded frame offset within the archive.
func ArchiveSource(offset uint64) ExtentSource { return ExtentSource{offset: offset} }

// ObjectSource identifies an external object by its decoded SHA-256 digest.
func ObjectSource(digest [32]byte) ExtentSource {
	var address [71]byte
	copy(address[:], "sha256:")
	hex.Encode(address[7:], digest[:])
	return ExtentSource{address: string(address[:])}
}

// ParseObjectSource accepts a canonical lowercase SHA-256 content address.
func ParseObjectSource(address string) (ExtentSource, error) {
	if _, err := ParseContentAddress(address); err != nil {
		return ExtentSource{}, err
	}
	return ExtentSource{address: address}, nil
}

// ArchiveOffset returns the frame offset and whether this is an archive source.
func (s ExtentSource) ArchiveOffset() (uint64, bool) { return s.offset, s.address == "" }

// ContentAddress returns the address and whether this is an external source.
func (s ExtentSource) ContentAddress() (string, bool) { return s.address, s.address != "" }

// String returns an object address or a decimal archive frame offset.
func (s ExtentSource) String() string {
	if s.address != "" {
		return s.address
	}
	return strconv.FormatUint(s.offset, 10)
}

func (s ExtentSource) value() any {
	if s.address != "" {
		return s.address
	}
	return s.offset
}
