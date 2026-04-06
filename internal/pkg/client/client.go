package client

import (
	"context"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

//nolint:interfacebloat // Application services depend on one aggregated Ceph client interface by design.
type Client interface {
	BIListByShard(ctx context.Context, containerName, bucketName string, shardID int) (*domain.BIList, error)
	BIListByObject(ctx context.Context, containerName, bucketName, objectName string, shardID int) (*domain.BIList, error)
	BucketLayout(ctx context.Context, containerName, bucketName string) (*domain.Layout, error)
	BucketStats(ctx context.Context, containerName, bucketName string) (*domain.BucketStats, error)
	GetDefaultZone(ctx context.Context, containerName string) (*domain.Zone, error)
	HasRawObject(ctx context.Context, containerName, pool, rawObject string) (bool, error)
	ListOmapKeys(ctx context.Context, containerName, indexPool, marker string, shard int) ([]*domain.BIIndex, error)
	ListBuckets(ctx context.Context, containerName string) ([]string, error)
	ObjectShard(ctx context.Context, containerName, objectName string, totalShards int) (*domain.ObjectShard, error)
	RemoveObject(ctx context.Context, containerName, bucketName, objectName, version string) error
	RemoveOmapKey(ctx context.Context, containerName, indexPool, marker string, shard int, key string) error
}
