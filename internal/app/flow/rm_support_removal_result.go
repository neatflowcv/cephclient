package flow

type RMSupportRemovalResult struct {
	omapKeys []string
}

func NewRMSupportRemovalResult(omapKeys []string) *RMSupportRemovalResult {
	copiedOmapKeys := make([]string, len(omapKeys))
	copy(copiedOmapKeys, omapKeys)

	return &RMSupportRemovalResult{
		omapKeys: copiedOmapKeys,
	}
}

func (r *RMSupportRemovalResult) OmapKeys() []string {
	copied := make([]string, len(r.omapKeys))
	copy(copied, r.omapKeys)

	return copied
}
