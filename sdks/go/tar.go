package zstar

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

func badTar(reason string) error      { return rejected("malformed-tar", reason) }
func tarMismatch(reason string) error { return rejected("tar-toc-mismatch", reason) }
func tarText(raw []byte) (string, error) {
	at := bytes.IndexByte(raw, 0)
	if at < 0 {
		at = len(raw)
	}
	if !allZero(raw[at:]) || !utf8.Valid(raw[:at]) {
		return "", badTar("header text encoding/padding")
	}
	return string(raw[:at]), nil
}

func allZero(raw []byte) bool {
	for _, b := range raw {
		if b != 0 {
			return false
		}
	}
	return true
}

func tarOctal(raw []byte) (uint64, error) {
	at := bytes.IndexByte(raw, 0)
	if at < 0 {
		at = len(raw)
	}
	for _, b := range raw[at:] {
		if b != 0 && b != ' ' {
			return 0, badTar("octal padding")
		}
	}
	text := strings.Trim(string(raw[:at]), " ")
	if text == "" {
		return 0, nil
	}
	for _, b := range []byte(text) {
		if b < '0' || b > '7' {
			return 0, badTar("octal syntax")
		}
	}
	n, err := strconv.ParseUint(text, 8, 64)
	if err != nil {
		return 0, badTar("octal overflow")
	}
	return n, nil
}

func tarDecimal(text string) (uint64, error) {
	if text == "" {
		return 0, badTar("decimal syntax")
	}
	for _, b := range []byte(text) {
		if b < '0' || b > '9' {
			return 0, badTar("decimal syntax")
		}
	}
	n, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, badTar("decimal overflow")
	}
	return n, nil
}

type tarHeader struct {
	name, link                  string
	kind                        byte
	mode, uid, gid, size, mtime uint64
}

func parseTarHeader(raw []byte) (tarHeader, error) {
	var h tarHeader
	if len(raw) != 512 || string(raw[257:265]) != "ustar\x0000" {
		return h, badTar("ustar signature")
	}
	var sum uint64
	for i, b := range raw {
		if i >= 148 && i < 156 {
			sum += 32
		} else {
			sum += uint64(b)
		}
	}
	check, err := tarOctal(raw[148:156])
	if err != nil {
		return h, err
	}
	if sum != check {
		return h, badTar("header checksum")
	}
	if !allZero(raw[265:329]) || !allZero(raw[500:]) {
		return h, badTar("owner/group names or header padding")
	}
	for _, field := range [][]byte{raw[329:337], raw[337:345]} {
		n, err := tarOctal(field)
		if err != nil {
			return h, err
		}
		if n != 0 {
			return h, badTar("device fields")
		}
	}
	h.name, err = tarText(raw[:100])
	if err != nil {
		return h, err
	}
	prefix, err := tarText(raw[345:500])
	if err != nil {
		return h, err
	}
	if prefix != "" {
		h.name = prefix + "/" + h.name
	}
	h.link, err = tarText(raw[157:257])
	if err != nil {
		return h, err
	}
	h.kind = raw[156]
	for _, field := range []struct {
		dst *uint64
		raw []byte
	}{
		{&h.mode, raw[100:108]},
		{&h.uid, raw[108:116]},
		{&h.gid, raw[116:124]},
		{&h.size, raw[124:136]},
		{&h.mtime, raw[136:148]},
	} {
		*field.dst, err = tarOctal(field.raw)
		if err != nil {
			return h, err
		}
	}
	return h, nil
}

func parsePAX(raw []byte) (map[string]string, error) {
	fields := make(map[string]string)
	if len(raw) == 0 {
		return nil, badTar("empty PAX")
	}
	for len(raw) > 0 {
		space := bytes.IndexByte(raw, ' ')
		if space < 0 {
			return nil, badTar("PAX length separator")
		}
		n, err := tarDecimal(string(raw[:space]))
		if err != nil {
			return nil, err
		}
		if n < uint64(space)+4 || n > uint64(len(raw)) || raw[n-1] != '\n' {
			return nil, badTar("PAX record bounds")
		}
		body := raw[space+1 : n-1]
		if !utf8.Valid(body) {
			return nil, badTar("PAX UTF-8")
		}
		key, value, ok := strings.Cut(string(body), "=")
		if !ok {
			return nil, badTar("PAX key/value")
		}
		switch key {
		case "path", "linkpath", "size", "uid", "gid", "mtime":
		default:
			return nil, badTar("unsupported PAX key")
		}
		if _, exists := fields[key]; exists {
			return nil, badTar("duplicate PAX key")
		}
		fields[key] = value
		raw = raw[n:]
	}
	return fields, nil
}

func paxMtime(text string) (int64, error) {
	negative := strings.HasPrefix(text, "-")
	unsigned := strings.TrimPrefix(text, "-")
	whole, fraction, decimal := strings.Cut(unsigned, ".")
	seconds, err := tarDecimal(whole)
	if err != nil {
		return 0, err
	}
	var nanos uint64
	if decimal {
		if fraction == "" {
			return 0, badTar("PAX mtime precision")
		}
		for i, b := range []byte(fraction) {
			if b < '0' || b > '9' || i >= 9 && b != '0' {
				return 0, badTar("PAX mtime precision")
			}
			if i < 9 {
				nanos = nanos*10 + uint64(b-'0')
			}
		}
		for i := len(fraction); i < 9; i++ {
			nanos *= 10
		}
	}
	limit := uint64(math.MaxInt64)
	if negative {
		limit++
	}
	if seconds > limit/1_000_000_000 || seconds == limit/1_000_000_000 && nanos > limit%1_000_000_000 {
		return 0, badTar("PAX mtime range")
	}
	magnitude := seconds*1_000_000_000 + nanos
	if negative {
		return -int64(magnitude), nil
	}
	return int64(magnitude), nil
}
