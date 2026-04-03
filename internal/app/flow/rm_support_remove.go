package flow

import (
	"context"
	"errors"
	"fmt"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

var errRMSupportOmapKeyStillExists = errors.New("omap key still exists after removal")

func (s *Service) RemoveRMSupportOmapKeys(
	ctx context.Context,
	containerName, indexPool, marker string,
	shard int,
	keys []string,
) (*RMSupportRemovalResult, error) {
	var currentOmapKeys []*domain.BIIndex

	for _, key := range keys {
		err := s.RemoveOmapKey(ctx, containerName, indexPool, marker, shard, key)
		if err != nil {
			return nil, fmt.Errorf("remove rm-support omap key %q: %w", key, err)
		}

		currentOmapKeys, err = s.ListOmapKeys(ctx, containerName, indexPool, marker, shard)
		if err != nil {
			return nil, fmt.Errorf("verify removed omap key %q: %w", key, err)
		}

		if omapKeyExists(currentOmapKeys, key) {
			return nil, fmt.Errorf("verify removed omap key %q: %w", key, errRMSupportOmapKeyStillExists)
		}
	}

	return NewRMSupportRemovalResult(currentOmapKeys), nil
}

func omapKeyExists(indexes []*domain.BIIndex, key string) bool {
	for _, index := range indexes {
		if index.Escaped() == key {
			return true
		}
	}

	return false
}
