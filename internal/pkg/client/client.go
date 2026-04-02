package client

import (
	"context"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type Client interface {
	BIListByShard(ctx context.Context, containerName, bucketName string, shardID int) (*domain.BIList, error)
	BIListByObject(ctx context.Context, containerName, bucketName, objectName string, shardID int) (*domain.BIList, error)
	BucketLayout(ctx context.Context, containerName, bucketName string) (*domain.Layout, error)
	BucketStats(ctx context.Context, containerName, bucketName string) (*domain.BucketStats, error)
	GetDefaultZone(ctx context.Context, containerName string) (*domain.Zone, error)
	ListOmapKeys(ctx context.Context, containerName, indexPool, marker string, shard int) ([]*domain.BIIndex, error)
	ListBuckets(ctx context.Context, containerName string) ([]string, error)
	ObjectShard(ctx context.Context, containerName, objectName string, totalShards int) (*domain.ObjectShard, error)
}
