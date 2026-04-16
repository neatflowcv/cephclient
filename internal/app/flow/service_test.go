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
	stats, err := service.BucketStats(ctx, "rgw", "test")

	// Assert
	require.NoError(t, err)
	require.Equal(t, "bucket-id", stats.ID())
	require.Equal(t, "test", stats.Name())
	require.Equal(t, 11, stats.TotalShards())
	require.Equal(t, "bucket-marker", stats.Marker())
	require.EqualValues(t, 5, stats.Size())
	require.Equal(t, 1, stats.ObjectCount())
	require.Equal(t, domain.VersioningStatusEnabled, stats.Versioning())
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
				false,
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
				false,
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

func TestServiceObjectInspectReadsEachStepInOrder(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	biList := domain.NewBIList([]domain.BIEntry{
		newVersionedPlainEntry("instance-1"),
		newVersionedInstanceEntry("test.txt", "instance-1"),
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
			[]domain.BIPendingLogEntry{
				domain.NewBIPendingLogEntry(8, []domain.BIPendingLogItem{
					domain.NewBIPendingLogItem(8, "unlink_olh", "tag-1", domain.NewBIOLHKey("test.txt", ""), false),
					domain.NewBIPendingLogItem(
						8,
						"remove_instance",
						"tag-1",
						domain.NewBIOLHKey("test.txt", "instance-pending"),
						false,
					),
				}),
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
	mockClient.BIListByObjectFunc = func(context.Context, string, string, string, int) (*domain.BIList, error) {
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
	mockClient.BIListByObjectFunc = func(context.Context, string, string, string, int) (*domain.BIList, error) {
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

func newVersionedPlainEntry(instance string) *domain.PlainBIEntry {
	return domain.NewPlainBIEntry(
		domain.NewBIIndex(fmt.Sprintf("%s:%s", "test.txt", instance)),
		domain.NewBIObjectEntry(
			"test.txt",
			instance,
			domain.NewBIVersion(8, 119),
			"",
			true,
			domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
			"",
			0,
			false,
			2,
		),
	)
}

func newVersionedInstanceEntry(name, instance string) *domain.InstanceBIEntry {
	return domain.NewInstanceBIEntry(
		domain.NewBIIndex(fmt.Sprintf("%s-instance:%s", name, instance)),
		domain.NewBIObjectEntry(
			name,
			instance,
			domain.NewBIVersion(8, 119),
			"",
			true,
			domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
			"",
			0,
			false,
			2,
		),
	)
}

func newOLHEntry(
	name, instance string,
	pendingLog []domain.BIPendingLogEntry,
) *domain.OLHBIEntry {
	return domain.NewOLHBIEntry(
		domain.NewBIIndex(name),
		domain.NewBIOLHEntry(
			domain.NewBIOLHKey(name, instance),
			false,
			2,
			pendingLog,
			"",
			true,
			false,
		),
	)
}
