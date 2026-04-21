package flow

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

var errRMSupportOmapKeyStillExists = errors.New("omap key still exists after removal")

func (s *Service) RemoveRMSupportOmapKeys(
	ctx context.Context,
	containerName, indexPool, marker string,
	shard int,
	keys []string,
) (*RMSupportRemovalResult, error) {
	var currentOmapKeys []string

	for _, key := range keys {
		err := s.RemoveOmapKey(ctx, containerName, indexPool, marker, shard, key)
		if err != nil {
			return nil, fmt.Errorf("remove rm-support omap key %q: %w", key, err)
		}

		resp, err := s.ListOmapKeys(ctx, ListOmapKeysRequest{
			ContainerName: containerName,
			IndexPool:     indexPool,
			Marker:        marker,
			ShardID:       shard,
		})
		if err != nil {
			return nil, fmt.Errorf("verify removed omap key %q: %w", key, err)
		}

		currentOmapKeys = resp.OmapKeys

		if omapKeyExists(currentOmapKeys, key) {
			return nil, fmt.Errorf("verify removed omap key %q: %w", key, errRMSupportOmapKeyStillExists)
		}
	}

	return NewRMSupportRemovalResult(currentOmapKeys), nil
}

func omapKeyExists(indexes []string, key string) bool {
	return slices.Contains(indexes, key)
}
