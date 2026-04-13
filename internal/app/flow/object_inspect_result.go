package flow

import "github.com/neatflowcv/cephclient/internal/pkg/domain"

type ObjectInspectResult struct {
	BIList      *domain.BIList
	DataPool    string
	Marker      string
	RawObjects  []*RawObjectExistence
	ShardID     int
	TotalShards int
}
