package flow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/client"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestServiceBucketStatsDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	var (
		gotContainerName string
		gotBucketName    string
	)

	service := flow.NewService(
		stubClient{
			bucketStats: func(_ context.Context, containerName, bucketName string) (*domain.BucketStats, error) {
				gotContainerName = containerName
				gotBucketName = bucketName

				return domain.NewBucketStats("bucket-id"), nil
			},
			listBuckets: nil,
		},
	)

	// Act
	stats, err := service.BucketStats(t.Context(), "rgw", "test")

	// Assert
	require.NoError(t, err)
	require.Equal(t, "rgw", gotContainerName)
	require.Equal(t, "test", gotBucketName)
	require.Equal(t, "bucket-id", stats.ID())
}

func TestServiceBucketStatsReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	wantErr := errClientFailed
	service := flow.NewService(stubClient{
		bucketStats: func(context.Context, string, string) (*domain.BucketStats, error) {
			return nil, wantErr
		},
		listBuckets: nil,
	})

	// Act
	_, err := service.BucketStats(t.Context(), "rgw", "test")

	// Assert
	require.ErrorIs(t, err, wantErr)
}

func TestServiceListBucketsDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	var gotContainerName string

	service := flow.NewService(stubClient{
		bucketStats: nil,
		listBuckets: func(_ context.Context, containerName string) ([]string, error) {
			gotContainerName = containerName

			return []string{"alpha", "beta"}, nil
		},
	})

	// Act
	buckets, err := service.ListBuckets(t.Context(), "rgw")

	// Assert
	require.NoError(t, err)
	require.Equal(t, "rgw", gotContainerName)
	require.Equal(t, []string{"alpha", "beta"}, buckets)
}

func TestServiceListBucketsReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	wantErr := errClientFailed
	service := flow.NewService(stubClient{
		bucketStats: nil,
		listBuckets: func(context.Context, string) ([]string, error) {
			return nil, wantErr
		},
	})

	// Act
	_, err := service.ListBuckets(t.Context(), "rgw")

	// Assert
	require.ErrorIs(t, err, wantErr)
}

type stubClient struct {
	bucketStats func(context.Context, string, string) (*domain.BucketStats, error)
	listBuckets func(context.Context, string) ([]string, error)
}

func (s stubClient) BucketStats(ctx context.Context, containerName, bucketName string) (*domain.BucketStats, error) {
	return s.bucketStats(ctx, containerName, bucketName)
}

func (s stubClient) ListBuckets(ctx context.Context, containerName string) ([]string, error) {
	return s.listBuckets(ctx, containerName)
}

var errClientFailed = errors.New("client failed")

var _ client.Client = stubClient{
	bucketStats: nil,
	listBuckets: nil,
}
