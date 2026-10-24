package zstar

import (
	"context"
	"errors"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// Stable values for FormatError.Category and ErrorCategory. They are untyped
// string constants so existing string comparisons keep compiling.
const (
	CategoryIntegrityFailure         = "integrity-failure"
	CategoryInvalidRange             = "invalid-range"
	CategoryMalformedExternalContent = "malformed-external-content"
	CategoryMalformedFooter          = "malformed-footer"
	CategoryMalformedFrame           = "malformed-frame"
	CategoryMalformedPointer         = "malformed-pointer"
	CategoryMalformedTar             = "malformed-tar"
	CategoryMalformedTOC             = "malformed-toc"
	CategoryMissingExternalContent   = "missing-external-content"
	CategoryNotZstar                 = "not-zstar"
	CategoryResourceLimit            = "resource-limit"
	CategoryTarTOCMismatch           = "tar-toc-mismatch"
	CategoryUnsupportedFeature       = "unsupported-feature"
	CategoryUnsupportedOperation     = "unsupported-operation"
	CategoryUnsupportedVersion       = "unsupported-version"
	// Returned by ErrorCategory for non-FormatError failures.
	CategoryCancelled = "cancelled"
	CategoryIO        = "io"
)

// FormatError exposes a stable rejection category without parsing diagnostics.
// Underlying transport errors remain available through errors.Is/errors.As.
type FormatError struct {
	Category string
	Cause    error
}

func (e *FormatError) Error() string         { return fmt.Sprintf("zstar: %s: %v", e.Category, e.Cause) }
func (e *FormatError) Unwrap() error         { return e.Cause }
func rejected(category, reason string) error { return &FormatError{category, errors.New(reason)} }
func categorized(category string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*FormatError](err); ok {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var depth *cbor.MaxNestedLevelError
	var array *cbor.MaxArrayElementsError
	var pairs *cbor.MaxMapPairsError
	if errors.As(err, &depth) || errors.As(err, &array) || errors.As(err, &pairs) {
		category = "resource-limit"
	}
	return &FormatError{category, err}
}

// ErrorCategory returns an empty string for success, a corpus rejection category
// for format/content errors, "cancelled" for context cancellation, or "io".
func ErrorCategory(err error) string {
	if err == nil {
		return ""
	}
	if format, ok := errors.AsType[*FormatError](err); ok {
		return format.Category
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return CategoryCancelled
	}
	return CategoryIO
}
