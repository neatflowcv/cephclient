package flow

import "github.com/neatflowcv/cephclient/internal/pkg/domain"

type RawObjectExistence struct {
	exists bool
	name   *domain.RawObjectName
}

func NewRawObjectExistence(name *domain.RawObjectName, exists bool) *RawObjectExistence {
	return &RawObjectExistence{
		exists: exists,
		name:   name,
	}
}

func (r *RawObjectExistence) Exists() bool {
	return r.exists
}

func (r *RawObjectExistence) Name() *domain.RawObjectName {
	return r.name
}
