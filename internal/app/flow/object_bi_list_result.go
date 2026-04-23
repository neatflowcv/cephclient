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

func newListBIByObjectResponseFromEntryGroup(entryGroup *domain.EntryGroup) *ListBIByObjectResponse {
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

	return NewListBIByObjectResponse(domain.NewBIList(entries))
}
