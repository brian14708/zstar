// Package zstar provides the native Go SDK for entry-oriented Zstar archives.
package zstar

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
)

// Footer contains the authenticated manifest location, lengths and digest.
type Footer struct {
	ManifestOffset, ManifestEncodedSize, ManifestDecodedSize uint64
	ManifestSHA256                                           [32]byte
}

// ParseFooter parses and validates an exact 80-byte archive footer.
func ParseFooter(data []byte) (Footer, error) {
	var f Footer
	if len(data) != 80 || binary.LittleEndian.Uint32(data[:4]) != 0x184D2A5F || string(data[8:16]) != "ZSTAR001" {
		return f, rejected("not-zstar", "footer signature")
	}
	if binary.LittleEndian.Uint32(data[4:8]) != 72 || binary.LittleEndian.Uint32(data[20:24]) != 0 {
		return f, rejected("malformed-footer", "footer lengths/reserved fields")
	}
	if binary.LittleEndian.Uint32(data[16:20]) != 1 {
		return f, rejected("unsupported-version", "footer version")
	}
	f.ManifestOffset = binary.LittleEndian.Uint64(data[24:32])
	f.ManifestEncodedSize = binary.LittleEndian.Uint64(data[32:40])
	f.ManifestDecodedSize = binary.LittleEndian.Uint64(data[40:48])
	copy(f.ManifestSHA256[:], data[48:80])
	if f.ManifestOffset == 0 || f.ManifestEncodedSize <= 8 || f.ManifestDecodedSize == 0 {
		return Footer{}, rejected("malformed-footer", "invalid footer bounds")
	}
	return f, nil
}

// VerifySHA256 checks a decoded buffer against the expected SHA-256 digest.
func VerifySHA256(data []byte, expected [32]byte) error {
	if sha256.Sum256(data) != expected {
		return rejected("integrity-failure", "digest mismatch")
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
