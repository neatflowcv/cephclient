package flow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestServiceBucketStatsDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	mockClient := &ClientMock{
		BucketStatsFunc: func(gotCtx context.Context, containerName, bucketName string) (*domain.BucketStats, error) {
			require.Equal(t, ctx, gotCtx)
			require.Equal(t, "rgw", containerName)
			require.Equal(t, "test", bucketName)

			return domain.NewBucketStats("bucket-id"), nil
		},
	}
	service := flow.NewService(mockClient)

	// Act
	stats, err := service.BucketStats(ctx, "rgw", "test")

	// Assert
	require.NoError(t, err)
	require.Equal(t, "bucket-id", stats.ID())
	require.Len(t, mockClient.BucketStatsCalls(), 1)
}

func TestServiceBucketStatsReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantErr := errClientFailed
	mockClient := &ClientMock{
		BucketStatsFunc: func(context.Context, string, string) (*domain.BucketStats, error) {
			return nil, wantErr
		},
	}
	service := flow.NewService(mockClient)

	// Act
	_, err := service.BucketStats(ctx, "rgw", "test")

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.BucketStatsCalls(), 1)
}

func TestServiceListBucketsDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	mockClient := &ClientMock{
		ListBucketsFunc: func(gotCtx context.Context, containerName string) ([]string, error) {
			require.Equal(t, ctx, gotCtx)
			require.Equal(t, "rgw", containerName)
			return []string{"alpha", "beta"}, nil
		},
	}
	service := flow.NewService(mockClient)

	// Act
	buckets, err := service.ListBuckets(ctx, "rgw")

	// Assert
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "beta"}, buckets)
	require.Len(t, mockClient.ListBucketsCalls(), 1)
}

func TestServiceListBucketsReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantErr := errClientFailed
	mockClient := &ClientMock{
		ListBucketsFunc: func(context.Context, string) ([]string, error) {
			return nil, wantErr
		},
	}
	service := flow.NewService(mockClient)

	// Act
	_, err := service.ListBuckets(ctx, "rgw")

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.ListBucketsCalls(), 1)
}

var errClientFailed = errors.New("client failed")
