package flow_test

import (
	"context"
	"errors"
	"fmt"
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

		return domain.NewBucketStats("bucket-id", "test", 11, "bucket-marker", 5, 1, domain.VersioningStatusEnabled)
	}
	service := flow.NewService(&mockClient)

	// Act
	stats, err := service.GetBucketStats(ctx, flow.GetBucketStatsRequest{
		ContainerName: "rgw",
		BucketName:    "test",
	})

	// Assert
	require.NoError(t, err)
	require.Equal(t, "rgw", stats.ContainerName)
	require.Equal(t, "test", stats.BucketName)
	require.Equal(t, "bucket-id", stats.ID)
	require.Equal(t, 11, stats.TotalShards)
	require.Equal(t, "bucket-marker", stats.Marker)
	require.EqualValues(t, 5, stats.Size)
	require.Equal(t, 1, stats.ObjectCount)
	require.Equal(t, domain.VersioningStatusEnabled, stats.Versioning)
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
	layout, err := service.GetBucketLayout(ctx, "rgw", "test")

	// Assert
	require.NoError(t, err)
	require.Equal(t, 1, layout.Generation())
	require.Len(t, mockClient.BucketLayoutCalls(), 1)
}

func TestServiceHasRawObjectDelegatesToClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.HasRawObjectFunc = func(
		gotCtx context.Context,
		containerName, pool, rawObject string,
	) (bool, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "default.rgw.buckets.data", pool)
		require.Equal(t, "marker__:version_object", rawObject)

		return true, nil
	}
	service := flow.NewService(&mockClient)

	exists, err := service.HasRawObject(ctx, "rgw", "default.rgw.buckets.data", "marker__:version_object")

	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, mockClient.HasRawObjectCalls(), 1)
}

func TestServiceBIListByShardDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantList := domain.NewBIList([]domain.BIEntry{
		domain.NewPlain(domain.DirParams{
			Name:             "test.txt",
			Instance:         "",
			Ver:              domain.NewBIVersion(-1, 0),
			Locator:          "",
			Exists:           false,
			Category:         0,
			Size:             0,
			MTime:            "0.000000",
			ETag:             "",
			StorageClass:     "",
			Owner:            "",
			OwnerDisplayName: "",
			ContentType:      "",
			AccountedSize:    0,
			UserData:         "",
			Appendable:       false,
			Tag:              "",
			Flags:            8,
			Pending:          false,
			VersionedEpoch:   0,
			IDX:              domain.NewBIIndex("test.txt"),
		}),
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
	biList, err := service.ListBIByShard(ctx, "rgw", "bucket-a", 3)

	// Assert
	require.NoError(t, err)
	require.Same(t, wantList, biList)
	require.Len(t, mockClient.BIListByShardCalls(), 1)
}

func TestServiceListBIByObjectDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantEntry := domain.NewPlain(domain.DirParams{
		Name:             "test.txt",
		Instance:         "",
		Ver:              domain.NewBIVersion(-1, 0),
		Locator:          "",
		Exists:           false,
		Category:         0,
		Size:             0,
		MTime:            "0.000000",
		ETag:             "",
		StorageClass:     "",
		Owner:            "",
		OwnerDisplayName: "",
		ContentType:      "",
		AccountedSize:    0,
		UserData:         "",
		Appendable:       false,
		Tag:              "",
		Flags:            8,
		Pending:          false,
		VersionedEpoch:   0,
		IDX:              domain.NewBIIndex("test.txt"),
	})

	var mockClient ClientMock

	mockClient.ListBucketIndexByObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName string,
		shardID int,
	) (*domain.EntryGroup, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, 3, shardID)

		return domain.NewEntryGroup(nil, []*domain.Plain{wantEntry}, nil), nil
	}
	service := flow.NewService(&mockClient)

	// Act
	shardID := 3
	resp, err := service.ListBIByObject(ctx, flow.ListBIByObjectRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
		ShardID:       &shardID,
		TotalShards:   nil,
	})

	// Assert
	require.NoError(t, err)
	require.Equal(t, []domain.BIEntry{wantEntry}, resp.BIList().Entries())
	require.Len(t, mockClient.ListBucketIndexByObjectCalls(), 1)
}

func TestServiceListBIByObjectResolvesShardWhenRequestShardIsNil(t *testing.T) {
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
		require.Equal(t, "bucket-a", bucketName)

		return domain.NewBucketStats("bucket-id", "bucket-a", 11, "bucket-marker", 5, 1, domain.VersioningStatusEnabled)
	}
	mockClient.ObjectShardFunc = func(
		gotCtx context.Context,
		containerName, objectName string,
		totalShards int,
	) (*domain.ObjectShard, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, 11, totalShards)

		return domain.NewObjectShard(7), nil
	}
	mockClient.ListBucketIndexByObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName string,
		shardID int,
	) (*domain.EntryGroup, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, 7, shardID)

		return domain.NewEntryGroup(nil, nil, nil), nil
	}
	service := flow.NewService(&mockClient)

	// Act
	resp, err := service.ListBIByObject(ctx, flow.ListBIByObjectRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
		ShardID:       nil,
		TotalShards:   nil,
	})

	// Assert
	require.NoError(t, err)
	require.Empty(t, resp.BIList().Entries())
	require.Len(t, mockClient.BucketStatsCalls(), 1)
	require.Len(t, mockClient.ObjectShardCalls(), 1)
	require.Len(t, mockClient.ListBucketIndexByObjectCalls(), 1)
}

func TestServiceListBIByObjectUsesRequestTotalShardsWhenProvided(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	totalShards := 13

	var mockClient ClientMock

	mockClient.ObjectShardFunc = func(
		gotCtx context.Context,
		containerName, objectName string,
		gotTotalShards int,
	) (*domain.ObjectShard, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, totalShards, gotTotalShards)

		return domain.NewObjectShard(5), nil
	}
	mockClient.ListBucketIndexByObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName string,
		shardID int,
	) (*domain.EntryGroup, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, 5, shardID)

		return domain.NewEntryGroup(nil, nil, nil), nil
	}
	service := flow.NewService(&mockClient)

	// Act
	resp, err := service.ListBIByObject(ctx, flow.ListBIByObjectRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
		ShardID:       nil,
		TotalShards:   &totalShards,
	})

	// Assert
	require.NoError(t, err)
	require.Empty(t, resp.BIList().Entries())
	require.Empty(t, mockClient.BucketStatsCalls())
	require.Len(t, mockClient.ObjectShardCalls(), 1)
	require.Len(t, mockClient.ListBucketIndexByObjectCalls(), 1)
}

func TestServicePurgeObjectResolvesShardWhenRequestTotalShardsIsNil(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	callOrder := make([]string, 0, 5)

	var mockClient ClientMock

	configurePurgeObjectBucketStatsMock(t, ctx, &mockClient, &callOrder, 11)
	configurePurgeObjectShardMock(t, ctx, &mockClient, &callOrder, 11, 7)
	configurePurgeObjectEmptyListMock(t, ctx, &mockClient, &callOrder, 7)
	service := flow.NewService(&mockClient)

	err := service.PurgeObject(ctx, flow.PurgeObjectRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
		TotalShards:   nil,
	})

	require.NoError(t, err)
	require.Equal(t, []string{"stats", "shard", "list", "list"}, callOrder)
	require.Len(t, mockClient.BucketStatsCalls(), 1)
	require.Len(t, mockClient.ObjectShardCalls(), 1)
	require.Len(t, mockClient.ListBucketIndexByObjectCalls(), 2)
	require.Empty(t, mockClient.RemoveObjectCalls())
	require.Empty(t, mockClient.RemoveRawObjectCalls())
	require.Empty(t, mockClient.RemoveOmapKeyCalls())
}

//nolint:funlen // Mock setup for purge verification requires multiple explicit assertions.
func TestServicePurgeObjectUsesRequestTotalShardsWhenProvided(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	totalShards := 13

	var mockClient ClientMock

	mockClient.ObjectShardFunc = func(
		gotCtx context.Context,
		containerName, objectName string,
		gotTotalShards int,
	) (*domain.ObjectShard, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, totalShards, gotTotalShards)

		return domain.NewObjectShard(5), nil
	}
	listCallCount := 0
	mockClient.ListBucketIndexByObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName string,
		shardID int,
	) (*domain.EntryGroup, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, 5, shardID)

		listCallCount++

		switch listCallCount {
		case 1:
			return domain.NewEntryGroup(nil, nil, nil), nil
		case 2:
			return domain.NewEntryGroup(nil, nil, nil), nil
		default:
			t.Fatalf("unexpected ListBucketIndexByObject call %d", listCallCount)

			return domain.NewEntryGroup(nil, nil, nil), errClientFailed
		}
	}
	mockClient.BucketStatsFunc = func(
		gotCtx context.Context,
		containerName, bucketName string,
	) (*domain.BucketStats, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)

		return domain.NewBucketStats(
			"bucket-id",
			"bucket-a",
			totalShards,
			"bucket-marker",
			5,
			1,
			domain.VersioningStatusEnabled,
		)
	}
	mockClient.GetDefaultZoneFunc = func(gotCtx context.Context, containerName string) (*domain.Zone, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)

		return domain.NewZone("default.rgw.buckets.data", "default.rgw.buckets.index"), nil
	}
	service := flow.NewService(&mockClient)

	// Act
	err := service.PurgeObject(ctx, flow.PurgeObjectRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
		TotalShards:   &totalShards,
	})

	// Assert
	require.NoError(t, err)
	require.Empty(t, mockClient.BucketStatsCalls())
	require.Empty(t, mockClient.GetDefaultZoneCalls())
	require.Len(t, mockClient.ObjectShardCalls(), 1)
	require.Len(t, mockClient.ListBucketIndexByObjectCalls(), 2)
	require.Empty(t, mockClient.RemoveObjectCalls())
	require.Empty(t, mockClient.RemoveRawObjectCalls())
	require.Empty(t, mockClient.RemoveOmapKeyCalls())
}

func TestServicePurgeObjectRemovesRemainingRawObjectsAndOmapKeysAfterVerification(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	totalShards := 13
	fixture := newPurgeObjectFallbackFixture(t, ctx)
	service := flow.NewService(&fixture.mockClient)

	// Act
	err := service.PurgeObject(ctx, flow.PurgeObjectRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
		TotalShards:   &totalShards,
	})

	// Assert
	require.NoError(t, err)
	require.Equal(
		t,
		[]string{"shard", "list", "remove", "list", "stats", "zone", "layout", "raw", "raw", "omap", "omap"},
		fixture.callOrder,
	)
	require.Equal(
		t,
		[]string{"bucket-marker_test.txt", "bucket-marker__:instance-1_test.txt"},
		fixture.rawObjects,
	)
	require.Equal(
		t,
		[]string{"test.txt", "test.txt-instance:instance-1"},
		fixture.omapKeys,
	)
	require.Len(t, fixture.mockClient.ListBucketIndexByObjectCalls(), 2)
	require.Len(t, fixture.mockClient.RemoveRawObjectCalls(), 2)
	require.Len(t, fixture.mockClient.BucketLayoutCalls(), 1)
	require.Len(t, fixture.mockClient.RemoveOmapKeyCalls(), 2)
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
	_, err := service.ListBIByShard(ctx, "rgw", "bucket-a", 3)

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.BIListByShardCalls(), 1)
}

func TestServiceListBIByObjectReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.ListBucketIndexByObjectFunc = func(
		context.Context,
		string,
		string,
		string,
		int,
	) (*domain.EntryGroup, error) {
		return nil, wantErr
	}
	service := flow.NewService(&mockClient)

	// Act
	shardID := 3
	_, err := service.ListBIByObject(ctx, flow.ListBIByObjectRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
		ShardID:       &shardID,
		TotalShards:   nil,
	})

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.ListBucketIndexByObjectCalls(), 1)
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
	_, err := service.GetBucketStats(ctx, flow.GetBucketStatsRequest{
		ContainerName: "rgw",
		BucketName:    "test",
	})

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.BucketStatsCalls(), 1)
}

func TestServiceHasRawObjectReturnsClientError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.HasRawObjectFunc = func(context.Context, string, string, string) (bool, error) {
		return false, wantErr
	}
	service := flow.NewService(&mockClient)

	exists, err := service.HasRawObject(ctx, "rgw", "default.rgw.buckets.data", "raw-object")

	require.ErrorIs(t, err, wantErr)
	require.False(t, exists)
	require.Len(t, mockClient.HasRawObjectCalls(), 1)
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
	_, err := service.GetBucketLayout(ctx, "rgw", "test")

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
	shard, err := service.GetObjectShard(ctx, "rgw", "test-object", 11)

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
	_, err := service.GetObjectShard(ctx, "rgw", "test-object", 11)

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.ObjectShardCalls(), 1)
}

func TestServiceObjectInspectReadsEachStepInOrder(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	biList := domain.NewBIList([]domain.BIEntry{
		newVersionedPlainEntry("instance-1"),
		newVersionedInstanceEntry(),
		newVersionedPlainEntry("instance-2"),
	})
	callOrder := make([]string, 0, 5)
	rawCalls := make([]string, 0, 3)
	mockClient := newObjectInspectClientMock(t, ctx, biList, &callOrder, &rawCalls)
	service := flow.NewService(mockClient)

	result, err := service.InspectObject(ctx, flow.InspectObjectRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
	})

	require.NoError(t, err)
	require.Equal(t, []string{"zone", "stats", "shard", "bilist", "raw", "raw", "raw"}, callOrder)
	require.Equal(t, "default.rgw.buckets.data", result.DataPool())
	require.Equal(t, "bucket-marker", result.Marker())
	require.Equal(t, 11, result.TotalShards())
	require.Equal(t, 3, result.ShardID())
	require.Same(t, biList, result.BIList())
	require.Equal(
		t,
		[]string{
			"bucket-marker_test.txt",
			"bucket-marker__:instance-1_test.txt",
			"bucket-marker__:instance-2_test.txt",
		},
		rawCalls,
	)
	require.Len(t, result.RawObjects(), 3)
	require.True(t, result.RawObjects()[0].Exists())
	require.False(t, result.RawObjects()[1].Exists())
	require.False(t, result.RawObjects()[2].Exists())
}

func TestServiceObjectInspectChecksOLHAndPendingLogVersionsInDataPool(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	biList := domain.NewBIList([]domain.BIEntry{
		newVersionedPlainEntry("instance-1"),
		newOLHEntry(
			"test.txt",
			"instance-olh",
			[]domain.PendingLogParams{
				{
					Key: 8,
					Val: []domain.PendingLogItemParams{
						{
							DeleteMarker: false,
							Epoch:        8,
							Instance:     "",
							Name:         "test.txt",
							Op:           "unlink_olh",
							OpTag:        "tag-1",
						},
						{
							DeleteMarker: false,
							Epoch:        8,
							Instance:     "instance-pending",
							Name:         "test.txt",
							Op:           "remove_instance",
							OpTag:        "tag-1",
						},
					},
				},
			},
		),
	})
	callOrder := make([]string, 0, 5)
	rawCalls := make([]string, 0, 4)
	mockClient := newObjectInspectClientMock(t, ctx, biList, &callOrder, &rawCalls)
	service := flow.NewService(mockClient)

	result, err := service.InspectObject(ctx, flow.InspectObjectRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
	})

	require.NoError(t, err)
	require.Equal(
		t,
		[]string{
			"bucket-marker_test.txt",
			"bucket-marker__:instance-1_test.txt",
			"bucket-marker__:instance-olh_test.txt",
			"bucket-marker__:instance-pending_test.txt",
		},
		rawCalls,
	)
	require.Len(t, result.RawObjects(), 4)
}

func TestServiceObjectInspectReturnsStepContextForBucketStats(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.GetDefaultZoneFunc = func(context.Context, string) (*domain.Zone, error) {
		return domain.NewZone("default.rgw.buckets.data", "default.rgw.buckets.index"), nil
	}
	mockClient.BucketStatsFunc = func(context.Context, string, string) (*domain.BucketStats, error) {
		return nil, wantErr
	}

	service := flow.NewService(&mockClient)

	_, err := service.InspectObject(ctx, flow.InspectObjectRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
	})

	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "read bucket stats")
}

func TestServiceObjectInspectReturnsStepContextForRawExists(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.GetDefaultZoneFunc = func(context.Context, string) (*domain.Zone, error) {
		return domain.NewZone("default.rgw.buckets.data", "default.rgw.buckets.index"), nil
	}
	mockClient.BucketStatsFunc = func(context.Context, string, string) (*domain.BucketStats, error) {
		return domain.NewBucketStats("bucket-id", "test", 11, "bucket-marker", 5, 1, domain.VersioningStatusEnabled)
	}
	mockClient.ObjectShardFunc = func(context.Context, string, string, int) (*domain.ObjectShard, error) {
		return domain.NewObjectShard(3), nil
	}
	mockClient.ListBIByObjectFunc = func(context.Context, string, string, string, int) (*domain.BIList, error) {
		return domain.NewBIList([]domain.BIEntry{newVersionedPlainEntry("instance-1")}), nil
	}
	mockClient.HasRawObjectFunc = func(context.Context, string, string, string) (bool, error) {
		return false, wantErr
	}

	service := flow.NewService(&mockClient)

	_, err := service.InspectObject(ctx, flow.InspectObjectRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
	})

	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "check raw object existence")
}

var errClientFailed = errors.New("client failed")

func newObjectInspectClientMock(
	t *testing.T,
	ctx context.Context,
	biList *domain.BIList,
	callOrder, rawCalls *[]string,
) *ClientMock {
	t.Helper()

	var mockClient ClientMock

	mockClient.GetDefaultZoneFunc = func(context.Context, string) (*domain.Zone, error) {
		*callOrder = append(*callOrder, "zone")

		return domain.NewZone("default.rgw.buckets.data", "default.rgw.buckets.index"), nil
	}
	mockClient.BucketStatsFunc = func(context.Context, string, string) (*domain.BucketStats, error) {
		*callOrder = append(*callOrder, "stats")

		return domain.NewBucketStats("bucket-id", "test", 11, "bucket-marker", 5, 1, domain.VersioningStatusEnabled)
	}
	mockClient.ObjectShardFunc = func(context.Context, string, string, int) (*domain.ObjectShard, error) {
		*callOrder = append(*callOrder, "shard")

		return domain.NewObjectShard(3), nil
	}
	mockClient.ListBIByObjectFunc = func(context.Context, string, string, string, int) (*domain.BIList, error) {
		*callOrder = append(*callOrder, "bilist")

		return biList, nil
	}
	mockClient.HasRawObjectFunc = func(
		gotCtx context.Context,
		containerName, pool, rawObject string,
	) (bool, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "default.rgw.buckets.data", pool)

		*callOrder = append(*callOrder, "raw")
		*rawCalls = append(*rawCalls, rawObject)

		return rawObject == "bucket-marker_test.txt", nil
	}

	return &mockClient
}

func newVersionedPlainEntry(instance string) *domain.Plain {
	return domain.NewPlain(domain.DirParams{
		Name:             "test.txt",
		Instance:         instance,
		Ver:              domain.NewBIVersion(8, 119),
		Locator:          "",
		Exists:           true,
		Category:         0,
		Size:             0,
		MTime:            "0.000000",
		ETag:             "",
		StorageClass:     "",
		Owner:            "",
		OwnerDisplayName: "",
		ContentType:      "",
		AccountedSize:    0,
		UserData:         "",
		Appendable:       false,
		Tag:              "",
		Flags:            0,
		Pending:          false,
		VersionedEpoch:   2,
		IDX:              domain.NewBIIndex(fmt.Sprintf("%s:%s", "test.txt", instance)),
	})
}

func configurePurgeObjectBucketStatsMock(
	t *testing.T,
	ctx context.Context,
	mockClient *ClientMock,
	callOrder *[]string,
	totalShards int,
) {
	t.Helper()

	mockClient.BucketStatsFunc = func(
		gotCtx context.Context,
		containerName, bucketName string,
	) (*domain.BucketStats, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)

		*callOrder = append(*callOrder, "stats")

		return domain.NewBucketStats(
			"bucket-id",
			"bucket-a",
			totalShards,
			"bucket-marker",
			5,
			1,
			domain.VersioningStatusEnabled,
		)
	}
}

func configurePurgeObjectShardMock(
	t *testing.T,
	ctx context.Context,
	mockClient *ClientMock,
	callOrder *[]string,
	totalShards int,
	shardID int,
) {
	t.Helper()

	mockClient.ObjectShardFunc = func(
		gotCtx context.Context,
		containerName, objectName string,
		gotTotalShards int,
	) (*domain.ObjectShard, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, totalShards, gotTotalShards)

		*callOrder = append(*callOrder, "shard")

		return domain.NewObjectShard(shardID), nil
	}
}

func configurePurgeObjectEmptyListMock(
	t *testing.T,
	ctx context.Context,
	mockClient *ClientMock,
	callOrder *[]string,
	shardID int,
) {
	t.Helper()

	mockClient.ListBucketIndexByObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName string,
		gotShardID int,
	) (*domain.EntryGroup, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, shardID, gotShardID)

		*callOrder = append(*callOrder, "list")

		return domain.NewEntryGroup(nil, nil, nil), nil
	}
}

type purgeObjectFallbackFixture struct {
	callOrder  []string
	mockClient ClientMock
	omapKeys   []string
	rawObjects []string
}

//nolint:funlen // Centralizes purge fallback mock wiring used by one focused test.
func newPurgeObjectFallbackFixture(
	t *testing.T,
	ctx context.Context,
) *purgeObjectFallbackFixture {
	t.Helper()

	//nolint:exhaustruct // Zero-value fixture is populated field-by-field below for readability.
	fixture := &purgeObjectFallbackFixture{}
	fixture.callOrder = make([]string, 0, 10)
	fixture.omapKeys = make([]string, 0, 2)
	fixture.rawObjects = make([]string, 0, 2)
	totalShards := 13

	fixture.mockClient.ObjectShardFunc = func(
		gotCtx context.Context,
		containerName, objectName string,
		gotTotalShards int,
	) (*domain.ObjectShard, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, totalShards, gotTotalShards)

		fixture.callOrder = append(fixture.callOrder, "shard")

		return domain.NewObjectShard(5), nil
	}

	listCallCount := 0
	fixture.mockClient.ListBucketIndexByObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName string,
		shardID int,
	) (*domain.EntryGroup, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, 5, shardID)

		fixture.callOrder = append(fixture.callOrder, "list")
		listCallCount++

		switch listCallCount {
		case 1:
			return domain.NewEntryGroup(
				nil,
				nil,
				[]*domain.Instance{
					newVersionedInstanceEntry(),
				},
			), nil
		case 2:
			return domain.NewEntryGroup(
				[]*domain.OLH{
					newOLHEntry("test.txt", "instance-1", nil),
				},
				nil,
				[]*domain.Instance{
					newVersionedInstanceEntry(),
				},
			), nil
		default:
			t.Fatalf("unexpected ListBucketIndexByObject call %d", listCallCount)

			return domain.NewEntryGroup(nil, nil, nil), errClientFailed
		}
	}
	fixture.mockClient.RemoveObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName, version string,
	) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)
		require.Equal(t, "test.txt", objectName)
		require.Equal(t, "instance-1", version)

		fixture.callOrder = append(fixture.callOrder, "remove")

		return nil
	}
	fixture.mockClient.BucketStatsFunc = func(
		gotCtx context.Context,
		containerName, bucketName string,
	) (*domain.BucketStats, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)

		fixture.callOrder = append(fixture.callOrder, "stats")

		return domain.NewBucketStats(
			"bucket-id",
			"bucket-a",
			totalShards,
			"bucket-marker",
			5,
			1,
			domain.VersioningStatusEnabled,
		)
	}
	fixture.mockClient.GetDefaultZoneFunc = func(gotCtx context.Context, containerName string) (*domain.Zone, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)

		fixture.callOrder = append(fixture.callOrder, "zone")

		return domain.NewZone("default.rgw.buckets.data", "default.rgw.buckets.index"), nil
	}
	fixture.mockClient.BucketLayoutFunc = func(
		gotCtx context.Context,
		containerName, bucketName string,
	) (*domain.Layout, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)

		fixture.callOrder = append(fixture.callOrder, "layout")

		return domain.NewLayout(2), nil
	}
	fixture.mockClient.RemoveRawObjectFunc = func(
		gotCtx context.Context,
		containerName, pool, rawObject string,
	) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "default.rgw.buckets.data", pool)

		fixture.callOrder = append(fixture.callOrder, "raw")
		fixture.rawObjects = append(fixture.rawObjects, rawObject)

		return nil
	}
	fixture.mockClient.RemoveOmapKeyFunc = func(
		gotCtx context.Context,
		containerName, indexPool string,
		rawObject string,
		key string,
	) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "default.rgw.buckets.index", indexPool)
		require.Equal(t, ".dir.bucket-marker.2.5", rawObject)

		fixture.callOrder = append(fixture.callOrder, "omap")
		fixture.omapKeys = append(fixture.omapKeys, key)

		return nil
	}

	return fixture
}

func newVersionedInstanceEntry() *domain.Instance {
	const (
		name     = "test.txt"
		instance = "instance-1"
	)

	return domain.NewInstance(domain.DirParams{
		Name:             name,
		Instance:         instance,
		Ver:              domain.NewBIVersion(8, 119),
		Locator:          "",
		Exists:           true,
		Category:         0,
		Size:             0,
		MTime:            "0.000000",
		ETag:             "",
		StorageClass:     "",
		Owner:            "",
		OwnerDisplayName: "",
		ContentType:      "",
		AccountedSize:    0,
		UserData:         "",
		Appendable:       false,
		Tag:              "",
		Flags:            0,
		Pending:          false,
		VersionedEpoch:   2,
		IDX:              domain.NewBIIndex(fmt.Sprintf("%s-instance:%s", name, instance)),
	})
}

func newOLHEntry(
	name, instance string,
	pendingLog []domain.PendingLogParams,
) *domain.OLH {
	return domain.NewOLH(domain.OLHParams{
		DeleteMarker:   false,
		Epoch:          2,
		Exists:         true,
		Instance:       instance,
		Name:           name,
		PendingLog:     pendingLog,
		PendingRemoval: false,
		Tag:            "",
		IDX:            domain.NewBIIndex(name),
	})
}
