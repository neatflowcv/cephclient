package client

import (
	"context"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type Client interface {
	BucketStats(ctx context.Context, containerName, bucketName string) (*domain.BucketStats, error)
}
