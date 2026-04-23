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
	Container string
	Bucket    string
	Object    string
	ShardID   int
	BIList    *domain.BIList
}

func newListBIByObjectResponse(
	req ListBIByObjectRequest,
	shardID int,
	entryGroup *domain.EntryGroup,
) *ListBIByObjectResponse {
	var entries []domain.BIEntry

	for _, entry := range entryGroup.OLHs() {
		entries = append(entries, entry)
	}

	for _, entry := range entryGroup.Plains() {
		entries = append(entries, entry)
	}

	for _, entry := range entryGroup.Instances() {
		entries = append(entries, entry)
	}

	return &ListBIByObjectResponse{
		Container: req.ContainerName,
		Bucket:    req.BucketName,
		Object:    req.ObjectName,
		ShardID:   shardID,
		BIList:    domain.NewBIList(entries),
	}
}
