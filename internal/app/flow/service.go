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
