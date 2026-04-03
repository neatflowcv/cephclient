package flow

import "github.com/neatflowcv/cephclient/internal/pkg/domain"

type RMSupportRemovalResult struct {
	omapKeys []*domain.BIIndex
}

func NewRMSupportRemovalResult(omapKeys []*domain.BIIndex) *RMSupportRemovalResult {
	copiedOmapKeys := make([]*domain.BIIndex, len(omapKeys))
	copy(copiedOmapKeys, omapKeys)

	return &RMSupportRemovalResult{
		omapKeys: copiedOmapKeys,
	}
}

func (r *RMSupportRemovalResult) OmapKeys() []*domain.BIIndex {
	copied := make([]*domain.BIIndex, len(r.omapKeys))
	copy(copied, r.omapKeys)

	return copied
}
