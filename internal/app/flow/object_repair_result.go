package flow

type RepairObjectRequest struct {
	MasterContainerName    string
	SecondaryContainerName string
	BucketName             string
	ObjectName             string
}

type RepairObjectResponse struct {
	secondaryRemovedOmapKeys []string
	masterRemovedOmapKeys    []string
}

func NewRepairObjectResponse(
	secondaryRemovedOmapKeys,
	masterRemovedOmapKeys []string,
) *RepairObjectResponse {
	copiedSecondaryRemovedOmapKeys := make([]string, len(secondaryRemovedOmapKeys))
	copy(copiedSecondaryRemovedOmapKeys, secondaryRemovedOmapKeys)

	copiedMasterRemovedOmapKeys := make([]string, len(masterRemovedOmapKeys))
	copy(copiedMasterRemovedOmapKeys, masterRemovedOmapKeys)

	return &RepairObjectResponse{
		secondaryRemovedOmapKeys: copiedSecondaryRemovedOmapKeys,
		masterRemovedOmapKeys:    copiedMasterRemovedOmapKeys,
	}
}

func (r *RepairObjectResponse) SecondaryRemovedOmapKeys() []string {
	copied := make([]string, len(r.secondaryRemovedOmapKeys))
	copy(copied, r.secondaryRemovedOmapKeys)

	return copied
}

func (r *RepairObjectResponse) MasterRemovedOmapKeys() []string {
	copied := make([]string, len(r.masterRemovedOmapKeys))
	copy(copied, r.masterRemovedOmapKeys)

	return copied
}
