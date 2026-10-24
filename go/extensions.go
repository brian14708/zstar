package zstar

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// Extensions retains unknown x- metadata at its original schema level.
// Values are integers, bool, UTF-8 strings, []byte, []any, or text-keyed maps.
// CBOR decoding uses uint64/int64 and map[any]any without losing integer bits.
type Extensions = map[string]any

func validateExtensions(ext Extensions) error {
	for key, value := range ext {
		if !strings.HasPrefix(key, "x-") || !utf8.ValidString(key) {
			return errors.New("zstar: extension keys must be UTF-8 and start with x-")
		}
		if err := validateExtensionValue(value, 0); err != nil {
			return err
		}
	}
	return nil
}

func validateExtensionValue(value any, depth int) error {
	if depth > 64 {
		return rejected("resource-limit", "extension nesting budget exceeded")
	}
	switch value := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, bool:
		return nil
	case string:
		if utf8.ValidString(value) {
			return nil
		}
	case []byte:
		if value != nil {
			return nil
		}
	case []any:
		if value == nil {
			break
		}
		for _, item := range value {
			if err := validateExtensionValue(item, depth+1); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		if value == nil {
			break
		}
		for key, item := range value {
			if !utf8.ValidString(key) {
				return errors.New("zstar: invalid extension text key")
			}
			if err := validateExtensionValue(item, depth+1); err != nil {
				return err
			}
		}
		return nil
	case map[any]any:
		if value == nil {
			break
		}
		for key, item := range value {
			text, ok := key.(string)
			if !ok || !utf8.ValidString(text) {
				return errors.New("zstar: extension map keys must be UTF-8 text")
			}
			if err := validateExtensionValue(item, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return errors.New("zstar: unsupported extension value")
}

func cloneExtensionValue(value any) any {
	switch value := value.(type) {
	case []byte:
		return append([]byte{}, value...)
	case []any:
		cloned := make([]any, len(value))
		for i, item := range value {
			cloned[i] = cloneExtensionValue(item)
		}
		return cloned
	case map[string]any:
		return cloneExtensions(value)
	case map[any]any:
		cloned := make(map[any]any, len(value))
		for key, item := range value {
			cloned[key] = cloneExtensionValue(item)
		}
		return cloned
	default:
		return value
	}
}

func cloneExtensions(ext Extensions) Extensions {
	if ext == nil {
		return nil
	}
	cloned := make(Extensions, len(ext))
	for key, value := range ext {
		cloned[key] = cloneExtensionValue(value)
	}
	return cloned
}
