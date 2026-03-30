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

func (s *Service) BucketStats(ctx context.Context, containerName, bucketName string) (*domain.BucketStats, error) {
	stats, err := s.client.BucketStats(ctx, containerName, bucketName)
	if err != nil {
		return nil, fmt.Errorf("get bucket stats: %w", err)
	}

	return stats, nil
}

func (s *Service) ListBuckets(ctx context.Context, containerName string) ([]string, error) {
	buckets, err := s.client.ListBuckets(ctx, containerName)
	if err != nil {
		return nil, fmt.Errorf("get bucket list: %w", err)
	}

	return buckets, nil
}
