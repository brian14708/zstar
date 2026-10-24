package zstar

import (
	"encoding/binary"
	"maps"
)

// Encode standard tuples directly. This avoids an interface tree for each
// entry/extent while the generic deterministic encoder handles x-* values.
func appendManifestHead(out []byte, major byte, value uint64) []byte {
	switch {
	case value < 24:
		return append(out, major<<5|byte(value))
	case value <= 255:
		return append(out, major<<5|24, byte(value))
	case value <= 65535:
		return binary.BigEndian.AppendUint16(append(out, major<<5|25), uint16(value))
	case value <= 0xffffffff:
		return binary.BigEndian.AppendUint32(append(out, major<<5|26), uint32(value))
	default:
		return binary.BigEndian.AppendUint64(append(out, major<<5|27), value)
	}
}

func appendManifestText(out []byte, text string) []byte {
	return append(appendManifestHead(out, 3, uint64(len(text))), text...)
}

func encodeEntry(entry Entry) ([]byte, error) {
	attributes := len(entry.Attributes) != 0 || entry.Linkname != nil
	fields := uint64(7)
	if attributes {
		fields++
	}
	out := appendManifestHead(make([]byte, 0, 64+len(entry.Path)+len(entry.Extents)*32), 4, fields)
	out = appendManifestText(out, entry.Path)
	for _, field := range []uint64{uint64(entry.Type), entry.Mode, entry.UID, entry.GID} {
		out = appendManifestHead(out, 0, field)
	}
	if entry.MtimeNS >= 0 {
		out = appendManifestHead(out, 0, uint64(entry.MtimeNS))
	} else {
		out = appendManifestHead(out, 1, uint64(-(entry.MtimeNS + 1)))
	}
	out = appendManifestHead(out, 4, uint64(len(entry.Extents)))
	for _, extent := range entry.Extents {
		fields := uint64(3)
		if extent.Offset != 0 {
			fields++
		}
		out = appendManifestHead(out, 4, fields)
		out = appendManifestHead(out, 0, extent.Size)
		if source, archive := extent.Source.ArchiveOffset(); archive {
			out = appendManifestHead(out, 0, source)
		} else {
			out = appendManifestText(out, extent.Source.address)
		}
		out = appendManifestHead(out, 0, extent.EncodedSize)
		if extent.Offset != 0 {
			out = appendManifestHead(out, 0, extent.Offset)
		}
		if len(out) > manifestEntryMax {
			return nil, rejected("malformed-toc", "entry maximum")
		}
	}
	if attributes {
		attrs := make(Extensions, len(entry.Attributes)+1)
		maps.Copy(attrs, entry.Attributes)
		if entry.Linkname != nil {
			attrs["linkname"] = *entry.Linkname
		}
		encoded, err := manifestEncode(attrs)
		if err != nil {
			return nil, err
		}
		out = append(out, encoded...)
	}
	if len(out) > manifestEntryMax {
		return nil, rejected("malformed-toc", "entry maximum")
	}
	return out, nil
}
