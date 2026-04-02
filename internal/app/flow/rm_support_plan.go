package flow

import "github.com/neatflowcv/cephclient/internal/pkg/domain"

type RMSupportPlan struct {
	biList    *domain.BIList
	indexPool string
	marker    string
	omapKeys  []*domain.BIIndex
	shardID   int
}

func NewRMSupportPlan(
	biList *domain.BIList,
	shardID int,
	marker, indexPool string,
	omapKeys []*domain.BIIndex,
) *RMSupportPlan {
	copiedOmapKeys := make([]*domain.BIIndex, len(omapKeys))
	copy(copiedOmapKeys, omapKeys)

	return &RMSupportPlan{
		biList:    biList,
		indexPool: indexPool,
		marker:    marker,
		omapKeys:  copiedOmapKeys,
		shardID:   shardID,
	}
}

func (p *RMSupportPlan) BIList() *domain.BIList {
	return p.biList
}

func (p *RMSupportPlan) IndexPool() string {
	return p.indexPool
}

func (p *RMSupportPlan) Marker() string {
	return p.marker
}

func (p *RMSupportPlan) OmapKeys() []*domain.BIIndex {
	copied := make([]*domain.BIIndex, len(p.omapKeys))
	copy(copied, p.omapKeys)

	return copied
}

func (p *RMSupportPlan) ShardID() int {
	return p.shardID
}
