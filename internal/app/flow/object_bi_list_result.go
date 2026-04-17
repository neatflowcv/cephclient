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
	biList *domain.BIList
}

func NewListBIByObjectResponse(biList *domain.BIList) *ListBIByObjectResponse {
	return &ListBIByObjectResponse{
		biList: biList,
	}
}

func (r *ListBIByObjectResponse) BIList() *domain.BIList {
	return r.biList
}
