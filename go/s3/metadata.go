package s3

import (
	"errors"
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
)

// ArchiveMetadata creates S3 user metadata from a completed archive's footer.
// It checks physical coverage; readers still validate the complete manifest.
func ArchiveMetadata(footer zstar.Footer, objectSize uint64) (map[string]string, error) {
	offset, _, err := footer.ManifestRange(objectSize)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		VersionMetadataKey:          "1",
		ManifestPositionMetadataKey: strconv.FormatUint(offset, 10) + ":" + strconv.FormatUint(footer.ManifestEncodedSize, 10),
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
	if seekErr != nil || end < start || end-start < zstar.FooterSize {
		return nil, nil //nolint:nilerr // A restored but unsized source can still be uploaded as a stream.
	}
	var raw [zstar.FooterSize]byte
	n, readErr := source.ReadAt(raw[:], end-zstar.FooterSize)
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
