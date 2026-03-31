package client

import (
	"context"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type Client interface {
	BIListByObject(ctx context.Context, containerName, bucketName, objectName string, shardID int) (*domain.BIList, error)
	BucketStats(ctx context.Context, containerName, bucketName string) (*domain.BucketStats, error)
	ListBuckets(ctx context.Context, containerName string) ([]string, error)
	ObjectShard(ctx context.Context, containerName, objectName string, totalShards int) (*domain.ObjectShard, error)
}
