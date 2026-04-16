package flow

import "github.com/neatflowcv/cephclient/internal/pkg/domain"

type InspectObjectRequest struct {
	ContainerName string
	BucketName    string
	ObjectName    string
}

type InspectObjectResponse struct {
	biList      *domain.BIList
	dataPool    string
	marker      string
	rawObjects  []*RawObjectExistence
	shardID     int
	totalShards int
}

func NewInspectObjectResponse(
	dataPool, marker string,
	totalShards, shardID int,
	biList *domain.BIList,
	rawObjects []*RawObjectExistence,
) *InspectObjectResponse {
	copiedRawObjects := make([]*RawObjectExistence, len(rawObjects))
	copy(copiedRawObjects, rawObjects)

	return &InspectObjectResponse{
		biList:      biList,
		dataPool:    dataPool,
		marker:      marker,
		rawObjects:  copiedRawObjects,
		shardID:     shardID,
		totalShards: totalShards,
	}
}

func (r *InspectObjectResponse) BIList() *domain.BIList {
	return r.biList
}

func (r *InspectObjectResponse) DataPool() string {
	return r.dataPool
}

func (r *InspectObjectResponse) Marker() string {
	return r.marker
}

func (r *InspectObjectResponse) RawObjects() []*RawObjectExistence {
	copied := make([]*RawObjectExistence, len(r.rawObjects))
	copy(copied, r.rawObjects)

	return copied
}

func (r *InspectObjectResponse) ShardID() int {
	return r.shardID
}

func (r *InspectObjectResponse) TotalShards() int {
	return r.totalShards
}
