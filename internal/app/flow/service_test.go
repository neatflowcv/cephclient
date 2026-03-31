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

	var mockClient ClientMock

	mockClient.BucketStatsFunc = func(
		gotCtx context.Context,
		containerName, bucketName string,
	) (*domain.BucketStats, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "test", bucketName)

		return domain.NewBucketStats("bucket-id", 11), nil
	}
	service := flow.NewService(&mockClient)

	// Act
	stats, err := service.BucketStats(ctx, "rgw", "test")

	// Assert
	require.NoError(t, err)
	require.Equal(t, "bucket-id", stats.ID())
	require.Equal(t, 11, stats.TotalShards())
	require.Len(t, mockClient.BucketStatsCalls(), 1)
}

func TestServiceBucketStatsReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.BucketStatsFunc = func(context.Context, string, string) (*domain.BucketStats, error) {
		return nil, wantErr
	}
	service := flow.NewService(&mockClient)

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

	var mockClient ClientMock

	mockClient.ListBucketsFunc = func(gotCtx context.Context, containerName string) ([]string, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)

		return []string{"alpha", "beta"}, nil
	}
	service := flow.NewService(&mockClient)

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

	var mockClient ClientMock

	mockClient.ListBucketsFunc = func(context.Context, string) ([]string, error) {
		return nil, wantErr
	}
	service := flow.NewService(&mockClient)

	// Act
	_, err := service.ListBuckets(ctx, "rgw")

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.ListBucketsCalls(), 1)
}

func TestServiceObjectShardDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()

	var mockClient ClientMock

	mockClient.ObjectShardFunc = func(
		gotCtx context.Context,
		containerName, objectName string,
		totalShards int,
	) (*domain.ObjectShard, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "test-object", objectName)
		require.Equal(t, 11, totalShards)

		return domain.NewObjectShard(3), nil
	}
	service := flow.NewService(&mockClient)

	// Act
	shard, err := service.ObjectShard(ctx, "rgw", "test-object", 11)

	// Assert
	require.NoError(t, err)
	require.Equal(t, 3, shard.Shard())
	require.Len(t, mockClient.ObjectShardCalls(), 1)
}

func TestServiceObjectShardReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.ObjectShardFunc = func(context.Context, string, string, int) (*domain.ObjectShard, error) {
		return nil, wantErr
	}
	service := flow.NewService(&mockClient)

	// Act
	_, err := service.ObjectShard(ctx, "rgw", "test-object", 11)

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.ObjectShardCalls(), 1)
}

var errClientFailed = errors.New("client failed")
