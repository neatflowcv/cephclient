package flow

import (
	"context"
	"fmt"

	"github.com/neatflowcv/cephclient/internal/pkg/client"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type Service struct {
	client client.Client
}

func NewService(client client.Client) *Service {
	return &Service{client: client}
}

func (s *Service) BIListByShard(
	ctx context.Context,
	containerName, bucketName string,
	shardID int,
) (*domain.BIList, error) {
	biList, err := s.client.BIListByShard(ctx, containerName, bucketName, shardID)
	if err != nil {
		return nil, fmt.Errorf("get bucket index list: %w", err)
	}

	return biList, nil
}

func (s *Service) BIListByObject(
	ctx context.Context,
	containerName, bucketName, objectName string,
	shardID int,
) (*domain.BIList, error) {
	biList, err := s.client.BIListByObject(ctx, containerName, bucketName, objectName, shardID)
	if err != nil {
		return nil, fmt.Errorf("get bucket index list: %w", err)
	}

	return biList, nil
}

func (s *Service) BucketStats(ctx context.Context, containerName, bucketName string) (*domain.BucketStats, error) {
	stats, err := s.client.BucketStats(ctx, containerName, bucketName)
	if err != nil {
		return nil, fmt.Errorf("get bucket stats: %w", err)
	}

	return stats, nil
}

func (s *Service) BucketLayout(ctx context.Context, containerName, bucketName string) (*domain.Layout, error) {
	layout, err := s.client.BucketLayout(ctx, containerName, bucketName)
	if err != nil {
		return nil, fmt.Errorf("get bucket layout: %w", err)
	}

	return layout, nil
}

func (s *Service) GetDefaultZone(ctx context.Context, containerName string) (*domain.Zone, error) {
	zone, err := s.client.GetDefaultZone(ctx, containerName)
	if err != nil {
		return nil, fmt.Errorf("get default zone: %w", err)
	}

	return zone, nil
}

func (s *Service) ListOmapKeys(
	ctx context.Context,
	containerName, indexPool, marker string,
	shard int,
) ([]*domain.BIIndex, error) {
	indexes, err := s.client.ListOmapKeys(ctx, containerName, indexPool, marker, shard)
	if err != nil {
		return nil, fmt.Errorf("get omap keys: %w", err)
	}

	return indexes, nil
}

func (s *Service) ListBuckets(ctx context.Context, containerName string) ([]string, error) {
	buckets, err := s.client.ListBuckets(ctx, containerName)
	if err != nil {
		return nil, fmt.Errorf("get bucket list: %w", err)
	}

	return buckets, nil
}

func (s *Service) ObjectShard(
	ctx context.Context,
	containerName, objectName string,
	totalShards int,
) (*domain.ObjectShard, error) {
	shard, err := s.client.ObjectShard(ctx, containerName, objectName, totalShards)
	if err != nil {
		return nil, fmt.Errorf("get object shard: %w", err)
	}

	return shard, nil
}

func (s *Service) RemoveObject(
	ctx context.Context,
	containerName, bucketName, objectName, version string,
) error {
	err := s.client.RemoveObject(ctx, containerName, bucketName, objectName, version)
	if err != nil {
		return fmt.Errorf("remove object: %w", err)
	}

	return nil
}

func (s *Service) RemoveOmapKey(
	ctx context.Context,
	containerName, indexPool, marker string,
	shard int,
	key string,
) error {
	err := s.client.RemoveOmapKey(ctx, containerName, indexPool, marker, shard, key)
	if err != nil {
		return fmt.Errorf("remove omap key: %w", err)
	}

	return nil
}

func (s *Service) RMSupportPlan(
	ctx context.Context,
	containerName, bucketName, objectName string,
	includeOmap bool,
) (*RMSupportPlan, error) {
	stats, err := s.BucketStats(ctx, containerName, bucketName)
	if err != nil {
		return nil, fmt.Errorf("read bucket stats: %w", err)
	}

	shard, err := s.ObjectShard(ctx, containerName, objectName, stats.TotalShards())
	if err != nil {
		return nil, fmt.Errorf("read object shard: %w", err)
	}

	biList, err := s.BIListByObject(ctx, containerName, bucketName, objectName, shard.Shard())
	if err != nil {
		return nil, fmt.Errorf("read bucket index list: %w", err)
	}

	if !includeOmap {
		zone, zoneErr := s.GetDefaultZone(ctx, containerName)
		if zoneErr != nil {
			return nil, fmt.Errorf("read default zone: %w", zoneErr)
		}

		return NewRMSupportPlan(biList, shard.Shard(), stats.Marker(), zone.IndexPool(), nil), nil
	}

	zone, err := s.GetDefaultZone(ctx, containerName)
	if err != nil {
		return nil, fmt.Errorf("read default zone: %w", err)
	}

	omapKeys, err := s.ListOmapKeys(ctx, containerName, zone.IndexPool(), stats.Marker(), shard.Shard())
	if err != nil {
		return nil, fmt.Errorf("list omap keys: %w", err)
	}

	return NewRMSupportPlan(biList, shard.Shard(), stats.Marker(), zone.IndexPool(), omapKeys), nil
}
