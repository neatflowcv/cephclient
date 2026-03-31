package podman

import (
	"errors"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

var (
	errZonePlacementPoolsEmpty         = errors.New("placement_pools is empty")
	errZoneIndexPoolEmpty              = errors.New("index_pool is empty")
	errZoneStandardStorageClassMissing = errors.New("STANDARD storage class not found")
	errZoneStandardDataPoolEmpty       = errors.New("STANDARD data_pool is empty")
)

type zoneResponse struct {
	PlacementPools []zonePlacementPool `json:"placement_pools"`
}

type zonePlacementPool struct {
	Val zonePlacementPoolValue `json:"val"`
}

type zonePlacementPoolValue struct {
	IndexPool      string                      `json:"index_pool"`
	StorageClasses map[string]zoneStorageClass `json:"storage_classes"`
}

type zoneStorageClass struct {
	DataPool string `json:"data_pool"`
}

func (r zoneResponse) toDomain() (*domain.Zone, error) {
	if len(r.PlacementPools) == 0 {
		return nil, errZonePlacementPoolsEmpty
	}

	pools := r.PlacementPools[0].Val
	if pools.IndexPool == "" {
		return nil, errZoneIndexPoolEmpty
	}

	standard, ok := pools.StorageClasses["STANDARD"]
	if !ok {
		return nil, errZoneStandardStorageClassMissing
	}

	if standard.DataPool == "" {
		return nil, errZoneStandardDataPoolEmpty
	}

	return domain.NewZone(standard.DataPool, pools.IndexPool), nil
}
