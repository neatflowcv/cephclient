package flow

import "github.com/neatflowcv/cephclient/internal/pkg/domain"

type GetBucketStatsRequest struct {
	ContainerName string
	BucketName    string
}

type GetBucketStatsResponse struct {
	ID          string
	Name        string
	TotalShards int
	Marker      string
	Size        int64
	ObjectCount int
	Versioning  domain.VersioningStatus
}

func newGetBucketStatsResponse(stats *domain.BucketStats) *GetBucketStatsResponse {
	return &GetBucketStatsResponse{
		ID:          stats.ID(),
		Name:        stats.Name(),
		TotalShards: stats.TotalShards(),
		Marker:      stats.Marker(),
		Size:        stats.Size(),
		ObjectCount: stats.ObjectCount(),
		Versioning:  stats.Versioning(),
	}
}
