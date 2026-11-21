package config

import (
	"errors"
	"fmt"
)

// FieldError is a validation failure anchored to a config field path.
// The path mirrors the YAML key hierarchy with numeric indexes for
// list entries ("routes[2].cache.fetch_timeout"), so an operator can
// locate the offending key directly in their config file.
//
// Error() renders as "config: <path>: <message>".
type FieldError struct {
	Path    string
	Message string
}

// Error implements the error interface.
func (e *FieldError) Error() string {
	return "config: " + e.Path + ": " + e.Message
}

// errCollector accumulates field errors across validation sections so
// one Parse reports every invalid field at once instead of forcing
// operators through fix-one-restart loops. Zero value is ready to use.
type errCollector struct {
	errs     []*FieldError
	warnings []*FieldError
}

// addf records a failure for the field at path, formatting the message
// like fmt.Errorf.
func (ec *errCollector) addf(path, format string, args ...any) {
	ec.errs = append(ec.errs, &FieldError{Path: path, Message: fmt.Sprintf(format, args...)})
}

// warnf records a non-fatal advisory for the field at path (config is
// valid but almost certainly not what the operator intended). Warnings
// surface via Config.Warnings and never fail validation.
func (ec *errCollector) warnf(path, format string, args ...any) {
	ec.warnings = append(ec.warnings, &FieldError{Path: path, Message: fmt.Sprintf(format, args...)})
}

// err returns nil, the single FieldError, or errors.Join of all
// collected errors.
func (ec *errCollector) err() error {
	switch len(ec.errs) {
	case 0:
		return nil
	case 1:
		return ec.errs[0]
	default:
		errs := make([]error, len(ec.errs))
		for i, e := range ec.errs {
			errs[i] = e
		}
		return errors.Join(errs...)
	}
}

// warningList returns the collected advisory warnings, or nil when
// there are none.
func (ec *errCollector) warningList() []*FieldError {
	if len(ec.warnings) == 0 {
		return nil
	}
	return ec.warnings
}

// unwrapAll flattens a (possibly joined) error tree into its leaves.
func unwrapAll(err error) []error {
	if err == nil {
		return nil
	}
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		var out []error
		for _, e := range joined.Unwrap() {
			out = append(out, unwrapAll(e)...)
		}
		return out
	}
	return []error{err}
}
