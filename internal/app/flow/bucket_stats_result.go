package flow

import "github.com/neatflowcv/cephclient/internal/pkg/domain"

type GetBucketStatsRequest struct {
	ContainerName string
	BucketName    string
}

type GetBucketStatsResponse struct {
	ContainerName string
	BucketName    string
	ID            string
	TotalShards   int
	Marker        string
	Size          int64
	ObjectCount   int
	Versioning    domain.VersioningStatus
}

func newGetBucketStatsResponse(
	req GetBucketStatsRequest,
	stats *domain.BucketStats,
) *GetBucketStatsResponse {
	return &GetBucketStatsResponse{
		ContainerName: req.ContainerName,
		BucketName:    stats.Name(),
		ID:            stats.ID(),
		TotalShards:   stats.TotalShards(),
		Marker:        stats.Marker(),
		Size:          stats.Size(),
		ObjectCount:   stats.ObjectCount(),
		Versioning:    stats.Versioning(),
	}
}
