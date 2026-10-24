package zstar

import (
	"errors"
	"fmt"
	"strings"
)

func validValue(value any) error {
	switch v := value.(type) {
	case uint64, int64, bool, string, []byte:
		return nil
	case []any:
		for _, x := range v {
			if err := validValue(x); err != nil {
				return err
			}
		}
		return nil
	case map[any]any:
		for k, x := range v {
			if _, ok := k.(string); !ok {
				return errors.New("zstar: CBOR map keys must be text")
			}
			if err := validValue(x); err != nil {
				return err
			}
		}
		return nil
	default:
		return errors.New("zstar: unsupported CBOR value")
	}
}

func schemaMap(value any, required, optional []string) (map[any]any, error) {
	m, ok := value.(map[any]any)
	if !ok {
		return nil, errors.New("zstar: expected map")
	}
	allowed := make(map[string]bool)
	for _, key := range required {
		allowed[key] = true
		if _, ok := m[key]; !ok {
			return nil, fmt.Errorf("zstar: missing field %q", key)
		}
	}
	for _, key := range optional {
		allowed[key] = true
	}
	for key := range m {
		s, ok := key.(string)
		if !ok || (!allowed[s] && !strings.HasPrefix(s, "x-")) {
			return nil, fmt.Errorf("zstar: unknown field %q", key)
		}
	}
	return m, nil
}

func validFeature(name string) bool {
	if len(name) == 0 || len(name) > 128 || name[0] < 'a' || name[0] > 'z' {
		return false
	}
	separator := false
	for _, c := range []byte(name) {
		if c == '.' || c == '-' {
			if separator {
				return false
			}
			separator = true
			continue
		}
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
		separator = false
	}
	return !separator
}
