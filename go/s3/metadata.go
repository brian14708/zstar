package s3

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	zstar "github.com/brian14708/zstar/go"
)

const (
	// VersionMetadataKey identifies the archive wire version in S3 user metadata.
	VersionMetadataKey = "zstar-version"
	// ManifestPositionMetadataKey contains decimal offset:encoded_size, including segment headers.
	ManifestPositionMetadataKey = "zstar-manifest-position"
	// ManifestSHA256MetadataKey contains the decoded manifest SHA-256 as lowercase hex.
	ManifestSHA256MetadataKey = "zstar-manifest-sha256"
)

// ArchiveMetadata creates S3 user metadata from a completed archive's footer.
// It checks physical coverage; readers still authenticate the manifest itself.
func ArchiveMetadata(footer zstar.Footer, objectSize uint64) (map[string]string, error) {
	if objectSize < 80 || footer.ManifestOffset == 0 || footer.ManifestOffset > objectSize-80 ||
		footer.ManifestEncodedSize <= 8 || footer.ManifestEncodedSize != objectSize-80-footer.ManifestOffset || footer.ManifestDecodedSize == 0 {
		return nil, errors.New("zstar s3: invalid archive footer coverage")
	}
	return map[string]string{
		VersionMetadataKey:          "1",
		ManifestPositionMetadataKey: strconv.FormatUint(footer.ManifestOffset, 10) + ":" + strconv.FormatUint(footer.ManifestEncodedSize, 10),
		ManifestSHA256MetadataKey:   fmt.Sprintf("%x", footer.ManifestSHA256),
	}, nil
}

func parseManifestPosition(metadata map[string]string) (offset, size uint64, ok bool) {
	if version, present := metadata[VersionMetadataKey]; present && version != "1" {
		return 0, 0, false
	}
	start, length, present := strings.Cut(metadata[ManifestPositionMetadataKey], ":")
	decimal := func(value string) bool {
		if value == "" {
			return false
		}
		for _, char := range value {
			if char < '0' || char > '9' {
				return false
			}
		}
		return true
	}
	if !present || !decimal(start) || !decimal(length) {
		return 0, 0, false
	}
	offset, err := strconv.ParseUint(start, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	size, err = strconv.ParseUint(length, 10, 64)
	return offset, size, err == nil
}

// Inspect ReaderAt-backed completed inputs without consuming the upload stream.
// Streaming and non-archive inputs continue to upload without inferred hints.
func inputArchiveMetadata(input io.Reader) (map[string]string, error) {
	source, ok := input.(interface {
		io.ReaderAt
		io.Seeker
	})
	if !ok {
		return nil, nil
	}
	start, err := source.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, nil //nolint:nilerr // Unsupported sizing disables optional hints; ordinary reads still work.
	}
	end, seekErr := source.Seek(0, io.SeekEnd)
	if _, err := source.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}
	if seekErr != nil || end < start || end-start < 80 {
		return nil, nil //nolint:nilerr // A restored but unsized source can still be uploaded as a stream.
	}
	var raw [80]byte
	n, readErr := source.ReadAt(raw[:], end-80)
	if n != len(raw) {
		if readErr != nil {
			return nil, readErr
		}
		return nil, io.ErrUnexpectedEOF
	}
	// ReaderAt permits EOF together with a complete final read.
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, readErr
	}
	footer, err := zstar.ParseFooter(raw[:])
	if err != nil {
		return nil, nil //nolint:nilerr // Generic inputs need not have a Zstar footer.
	}
	metadata, err := ArchiveMetadata(footer, uint64(end-start))
	if err != nil {
		return nil, nil //nolint:nilerr // Non-archive inputs upload normally without inferred metadata.
	}
	return metadata, nil
}
