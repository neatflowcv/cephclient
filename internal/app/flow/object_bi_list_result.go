package flow

import "github.com/neatflowcv/cephclient/internal/pkg/domain"

type ListBIByObjectRequest struct {
	ContainerName string
	BucketName    string
	ObjectName    string
	ShardID       *int
	TotalShards   *int
}

type ListBIByObjectResponse struct {
	Container  string
	Bucket     string
	Object     string
	ShardID    int
	EntryGroup *domain.EntryGroup
}

func newListBIByObjectResponse(
	req ListBIByObjectRequest,
	shardID int,
	entryGroup *domain.EntryGroup,
) *ListBIByObjectResponse {
	if entryGroup == nil {
		entryGroup = domain.NewEntryGroup(nil, nil, nil)
	}

	return &ListBIByObjectResponse{
		Container:  req.ContainerName,
		Bucket:     req.BucketName,
		Object:     req.ObjectName,
		ShardID:    shardID,
		EntryGroup: entryGroup,
	}
}
