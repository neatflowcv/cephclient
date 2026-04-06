package flow

import "github.com/neatflowcv/cephclient/internal/pkg/domain"

type ObjectInspectResult struct {
	biList      *domain.BIList
	dataPool    string
	marker      string
	rawObjects  []*RawObjectExistence
	shardID     int
	totalShards int
}

func NewObjectInspectResult(
	dataPool, marker string,
	totalShards, shardID int,
	biList *domain.BIList,
	rawObjects []*RawObjectExistence,
) *ObjectInspectResult {
	copiedRawObjects := make([]*RawObjectExistence, len(rawObjects))
	copy(copiedRawObjects, rawObjects)

	return &ObjectInspectResult{
		biList:      biList,
		dataPool:    dataPool,
		marker:      marker,
		rawObjects:  copiedRawObjects,
		shardID:     shardID,
		totalShards: totalShards,
	}
}

func (r *ObjectInspectResult) BIList() *domain.BIList {
	return r.biList
}

func (r *ObjectInspectResult) DataPool() string {
	return r.dataPool
}

func (r *ObjectInspectResult) Marker() string {
	return r.marker
}

func (r *ObjectInspectResult) RawObjects() []*RawObjectExistence {
	copied := make([]*RawObjectExistence, len(r.rawObjects))
	copy(copied, r.rawObjects)

	return copied
}

func (r *ObjectInspectResult) ShardID() int {
	return r.shardID
}

func (r *ObjectInspectResult) TotalShards() int {
	return r.totalShards
}
