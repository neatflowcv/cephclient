package domain

import (
	"errors"
	"fmt"
)

type Versioning string

const (
	VersioningOff       Versioning = "off"
	VersioningSuspended Versioning = "suspended"
	VersioningEnabled   Versioning = "enabled"
)

var errInvalidVersioning = errors.New("invalid versioning")

func NewVersioning(value string) (Versioning, error) {
	versioning := Versioning(value)

	switch versioning {
	case VersioningOff, VersioningSuspended, VersioningEnabled:
		return versioning, nil
	default:
		return "", fmt.Errorf("%w: %q", errInvalidVersioning, value)
	}
}
