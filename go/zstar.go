// Package zstar provides the native Go SDK for entry-oriented Zstar archives.
package zstar

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
)

// FooterSize is the fixed terminal skippable frame size, including its header.
const FooterSize = 28

// Footer contains metadata lengths. Its position is derived from archive size.
type Footer struct {
	ManifestEncodedSize, ManifestDecodedSize uint64
}

// ManifestRange derives the metadata offset and encoded size from the complete archive size.
// It rejects underflow, empty data regions, and invalid metadata lengths.
func (f Footer) ManifestRange(archiveSize uint64) (offset, encodedSize uint64, err error) {
	if archiveSize < FooterSize || f.ManifestEncodedSize <= 8 || f.ManifestDecodedSize == 0 || f.ManifestEncodedSize >= archiveSize-FooterSize {
		return 0, 0, rejected(CategoryMalformedFooter, "metadata coverage")
	}
	return archiveSize - FooterSize - f.ManifestEncodedSize, f.ManifestEncodedSize, nil
}

// ParseFooter parses and validates an exact 28-byte archive footer.
// Call ManifestRange to validate coverage against the complete archive size.
func ParseFooter(data []byte) (Footer, error) {
	var f Footer
	if len(data) != FooterSize || binary.LittleEndian.Uint32(data[:4]) != 0x184D2A5F || string(data[24:]) != "Z*01" {
		return f, rejected(CategoryNotZstar, "footer signature")
	}
	if binary.LittleEndian.Uint32(data[4:8]) != 20 {
		return f, rejected(CategoryMalformedFooter, "footer payload length")
	}
	f.ManifestEncodedSize = binary.LittleEndian.Uint64(data[8:16])
	f.ManifestDecodedSize = binary.LittleEndian.Uint64(data[16:24])
	if f.ManifestEncodedSize <= 8 || f.ManifestDecodedSize == 0 {
		return Footer{}, rejected(CategoryMalformedFooter, "invalid footer bounds")
	}
	return f, nil
}

// verifySHA256 checks a decoded buffer against the expected SHA-256 digest.
func verifySHA256(data []byte, expected [32]byte) error {
	if sha256.Sum256(data) != expected {
		return rejected(CategoryIntegrityFailure, "digest mismatch")
	}
	return nil
}

// RangeReader reads exact half-open byte ranges from an immutable source.
// Implementations must support concurrent calls. Returned bytes are owned by
// the caller. The caller owns the source; readers never close it.
type RangeReader interface {
	Size(ctx context.Context) (int64, error)
	ReadRange(ctx context.Context, offset, length int64) ([]byte, error)
}

// ManifestRangeSource optionally supplies the encoded manifest location,
// excluding the footer. Readers validate the hint against the terminal footer.
type ManifestRangeSource interface {
	ManifestRange(ctx context.Context) (offset, encodedSize uint64, present bool, err error)
}
