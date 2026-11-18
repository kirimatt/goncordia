package driver

import "errors"

// Stable error categories let callers use errors.Is without matching backend
// error strings.
var (
	ErrUnsupported   = errors.New("goncordia: operation unsupported")
	ErrNotFound      = errors.New("goncordia: not found")
	ErrConflict      = errors.New("goncordia: conflict")
	ErrStaleClaim    = errors.New("goncordia: stale claim")
	ErrInvalidCursor = errors.New("goncordia: invalid cursor")
	// ErrResultLost is returned by JobResultGet when a result was stored but
	// is gone without being deleted, for example evicted by a cache policy.
	// Drivers wrap it together with ErrNotFound.
	ErrResultLost = errors.New("goncordia: stored result lost")
)
