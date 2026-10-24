package s3

import (
	"path"
	"strings"
)

const (
	zstdContentType       = "application/zstd"
	mutableCacheControl   = "private, no-cache, no-transform"
	immutableCacheControl = "private, max-age=31536000, immutable, no-transform"
)

func artifactContentType(key string) string {
	extension := strings.TrimPrefix(path.Ext(key), ".")
	switch strings.ToLower(extension) {
	case "zstar", "zst", "zstp":
		return zstdContentType
	case "cbor":
		return "application/cbor"
	default:
		return "application/octet-stream"
	}
}

func cacheControl(immutable bool) string {
	if immutable {
		return immutableCacheControl
	}
	return mutableCacheControl
}
