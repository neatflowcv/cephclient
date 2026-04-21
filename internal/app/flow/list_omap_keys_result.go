package flow

import "github.com/neatflowcv/cephclient/internal/pkg/domain"

type ListOmapKeysRequest struct {
	ContainerName string
	IndexPool     string
	Marker        string
	ShardID       int
}

type ListOmapKeysResponse struct {
	OmapKeys []string
}

func newListOmapKeysResponse(indexes []*domain.BIIndex) *ListOmapKeysResponse {
	var omapKeys []string
	for _, omapKey := range indexes {
		omapKeys = append(omapKeys, omapKey.Escaped())
	}

	return &ListOmapKeysResponse{
		OmapKeys: omapKeys,
	}
}
