package domain

import (
	"errors"
	"fmt"
)

type VersioningStatus string

const (
	VersioningStatusOff       VersioningStatus = "off"
	VersioningStatusSuspended VersioningStatus = "suspended"
	VersioningStatusEnabled   VersioningStatus = "enabled"
)

var errInvalidVersioning = errors.New("invalid versioning")

func (status VersioningStatus) Validate() error {
	switch status {
	case VersioningStatusOff, VersioningStatusSuspended, VersioningStatusEnabled:
		return nil
	default:
		return fmt.Errorf("%w: %q", errInvalidVersioning, status)
	}
}
