package driver

import "errors"

// Stable error categories let callers use errors.Is without matching backend
// error strings.
var (
	ErrUnsupported = errors.New("goncordia: operation unsupported")
	ErrNotFound    = errors.New("goncordia: not found")
	ErrConflict    = errors.New("goncordia: conflict")
	ErrStaleClaim  = errors.New("goncordia: stale claim")
)
