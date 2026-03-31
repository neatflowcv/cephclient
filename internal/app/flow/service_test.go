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

func TestServiceBucketLayoutDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()

	var mockClient ClientMock

	mockClient.BucketLayoutFunc = func(
		gotCtx context.Context,
		containerName, bucketName string,
	) (*domain.Layout, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "test", bucketName)

		return domain.NewLayout(1), nil
	}
	service := flow.NewService(&mockClient)

	// Act
	layout, err := service.BucketLayout(ctx, "rgw", "test")

	// Assert
	require.NoError(t, err)
	require.Equal(t, 1, layout.Generation())
	require.Len(t, mockClient.BucketLayoutCalls(), 1)
}

func TestServiceBIListByShardDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantList := domain.NewBIList([]domain.BIEntry{
		domain.NewPlainBIEntry(
			domain.NewBIIndex("test.txt"),
			domain.NewBIObjectEntry(
				"test.txt",
				"",
				domain.NewBIVersion(-1, 0),
				"",
				false,
				domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
				"",
				8,
				nil,
				0,
			),
		),
	})

	var mockClient ClientMock

	mockClient.BIListByShardFunc = func(
		gotCtx context.Context,
		containerName, bucketName string,
		shardID int,
	) (*domain.BIList, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)
		require.Equal(t, 3, shardID)

		return wantList, nil
	}
	service := flow.NewService(&mockClient)

	// Act
	biList, err := service.BIListByShard(ctx, "rgw", "bucket-a", 3)

	// Assert
	require.NoError(t, err)
	require.Same(t, wantList, biList)
	require.Len(t, mockClient.BIListByShardCalls(), 1)
}

func TestServiceBIListByObjectDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantList := domain.NewBIList([]domain.BIEntry{
		domain.NewPlainBIEntry(
			domain.NewBIIndex("test.txt"),
			domain.NewBIObjectEntry(
				"test.txt",
				"",
				domain.NewBIVersion(-1, 0),
				"",
				false,
				domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
				"",
				8,
				nil,
				0,
			),
		),
	})

	var mockClient ClientMock

	mockClient.BIListByObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName string,
		shardID int,
	) (*domain.BIList, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, 3, shardID)

		return wantList, nil
	}
	service := flow.NewService(&mockClient)

	// Act
	biList, err := service.BIListByObject(ctx, "rgw", "bucket-a", "test.txt", 3)

	// Assert
	require.NoError(t, err)
	require.Same(t, wantList, biList)
	require.Len(t, mockClient.BIListByObjectCalls(), 1)
}

func TestServiceBIListByShardReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.BIListByShardFunc = func(context.Context, string, string, int) (*domain.BIList, error) {
		return nil, wantErr
	}
	service := flow.NewService(&mockClient)

	// Act
	_, err := service.BIListByShard(ctx, "rgw", "bucket-a", 3)

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.BIListByShardCalls(), 1)
}

func TestServiceBIListByObjectReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.BIListByObjectFunc = func(context.Context, string, string, string, int) (*domain.BIList, error) {
		return nil, wantErr
	}
	service := flow.NewService(&mockClient)

	// Act
	_, err := service.BIListByObject(ctx, "rgw", "bucket-a", "test.txt", 3)

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.BIListByObjectCalls(), 1)
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

func TestServiceBucketLayoutReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.BucketLayoutFunc = func(context.Context, string, string) (*domain.Layout, error) {
		return nil, wantErr
	}
	service := flow.NewService(&mockClient)

	// Act
	_, err := service.BucketLayout(ctx, "rgw", "test")

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.BucketLayoutCalls(), 1)
}

func TestServiceGetDefaultZoneDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()

	var mockClient ClientMock

	mockClient.GetDefaultZoneFunc = func(gotCtx context.Context, containerName string) (*domain.Zone, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)

		return domain.NewZone("test.rgw.buckets.data", "test.rgw.buckets.index"), nil
	}
	service := flow.NewService(&mockClient)

	// Act
	zone, err := service.GetDefaultZone(ctx, "rgw")

	// Assert
	require.NoError(t, err)
	require.Equal(t, "test.rgw.buckets.data", zone.DataPool())
	require.Equal(t, "test.rgw.buckets.index", zone.IndexPool())
	require.Len(t, mockClient.GetDefaultZoneCalls(), 1)
}

func TestServiceGetDefaultZoneReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.GetDefaultZoneFunc = func(context.Context, string) (*domain.Zone, error) {
		return nil, wantErr
	}
	service := flow.NewService(&mockClient)

	// Act
	_, err := service.GetDefaultZone(ctx, "rgw")

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.GetDefaultZoneCalls(), 1)
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
