package flow_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

const (
	repairObjectMasterContainer    = "rgw-master"
	repairObjectSecondaryContainer = "rgw-secondary"
	repairObjectBucket             = "bucket-a"
	repairObjectName               = "object-a"
)

func TestServiceRepairObjectPlansSecondaryAndMasterOmapRepair(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()

	var mockClient ClientMock

	configureRepairObjectIndexMocks(t, ctx, &mockClient, "version-a", "version-a")
	configureRepairObjectZoneMocks(t, ctx, &mockClient)
	configureRepairObjectLayoutMocks(t, ctx, &mockClient)
	configureRepairObjectRawObjectMissingMock(t, ctx, &mockClient)
	configureRepairObjectRemoveOmapKeyMock(t, ctx, &mockClient)

	service := flow.NewService(&mockClient)

	// Act
	result, err := service.RepairObject(ctx, flow.RepairObjectRequest{
		MasterContainerName:    repairObjectMasterContainer,
		SecondaryContainerName: repairObjectSecondaryContainer,
		BucketName:             repairObjectBucket,
		ObjectName:             repairObjectName,
	})

	// Assert
	require.NoError(t, err)
	require.Equal(t, []string{
		repairObjectName + ":secondary-instance-only",
		repairObjectName + ":secondary-plain-only",
	}, result.SecondaryRemovedOmapKeys())
	require.Equal(t, []string{
		repairObjectName + ":master-instance-only",
		repairObjectName + ":master-plain-only",
	}, result.MasterRemovedOmapKeys())
	require.Len(t, mockClient.ListBucketIndexByObjectCalls(), 2)
	require.Empty(t, mockClient.RemoveObjectCalls())
	require.Len(t, mockClient.RemoveOmapKeyCalls(), 4)
}

func TestServiceRepairObjectReturnsErrorWhenOLHInstancesDiffer(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()

	var mockClient ClientMock

	configureRepairObjectIndexMocks(t, ctx, &mockClient, "master-version", "secondary-version")

	service := flow.NewService(&mockClient)

	// Act
	_, err := service.RepairObject(ctx, flow.RepairObjectRequest{
		MasterContainerName:    repairObjectMasterContainer,
		SecondaryContainerName: repairObjectSecondaryContainer,
		BucketName:             repairObjectBucket,
		ObjectName:             repairObjectName,
	})

	// Assert
	require.EqualError(
		t,
		err,
		"object repair olh instance mismatch: master=[master-version] secondary=[secondary-version]",
	)
	require.Empty(t, mockClient.RemoveOmapKeyCalls())
	require.Empty(t, mockClient.RemoveObjectCalls())
}

func TestServiceRepairObjectSkipsCleanupWhenNoOrphansExist(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()

	var mockClient ClientMock

	configureRepairObjectNoOrphanIndexMocks(t, ctx, &mockClient)

	service := flow.NewService(&mockClient)

	// Act
	result, err := service.RepairObject(ctx, flow.RepairObjectRequest{
		MasterContainerName:    repairObjectMasterContainer,
		SecondaryContainerName: repairObjectSecondaryContainer,
		BucketName:             repairObjectBucket,
		ObjectName:             repairObjectName,
	})

	// Assert
	require.NoError(t, err)
	require.Empty(t, result.SecondaryRemovedOmapKeys())
	require.Empty(t, result.MasterRemovedOmapKeys())
	require.Empty(t, mockClient.GetDefaultZoneCalls())
	require.Empty(t, mockClient.RemoveOmapKeyCalls())
	require.Empty(t, mockClient.RemoveObjectCalls())
}

func TestServiceRepairObjectStopsWhenSecondaryRawObjectExists(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()

	var mockClient ClientMock

	configureRepairObjectIndexMocks(t, ctx, &mockClient, "version-a", "version-a")
	configureRepairObjectZoneMocks(t, ctx, &mockClient)
	configureRepairObjectRawObjectExistsMock(t, ctx, &mockClient)

	service := flow.NewService(&mockClient)

	// Act
	_, err := service.RepairObject(ctx, flow.RepairObjectRequest{
		MasterContainerName:    repairObjectMasterContainer,
		SecondaryContainerName: repairObjectSecondaryContainer,
		BucketName:             repairObjectBucket,
		ObjectName:             repairObjectName,
	})

	// Assert
	require.EqualError(
		t,
		err,
		"repair secondary object: object repair raw object exists: "+
			"raw_object=secondary-marker__:secondary-instance-only_object-a",
	)
	require.Empty(t, mockClient.GetBucketLayoutCalls())
	require.Empty(t, mockClient.RemoveOmapKeyCalls())
	require.Empty(t, mockClient.RemoveObjectCalls())
}

func TestServiceRepairObjectStopsWhenMasterRawObjectExists(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()

	var mockClient ClientMock

	configureRepairObjectMasterOrphanIndexMocks(t, ctx, &mockClient)
	configureRepairObjectZoneMocks(t, ctx, &mockClient)
	configureRepairObjectLayoutMocks(t, ctx, &mockClient)
	configureRepairObjectMasterRawObjectExistsMock(t, ctx, &mockClient)

	service := flow.NewService(&mockClient)

	// Act
	_, err := service.RepairObject(ctx, flow.RepairObjectRequest{
		MasterContainerName:    repairObjectMasterContainer,
		SecondaryContainerName: repairObjectSecondaryContainer,
		BucketName:             repairObjectBucket,
		ObjectName:             repairObjectName,
	})

	// Assert
	require.EqualError(
		t,
		err,
		"repair master object: object repair raw object exists: "+
			"raw_object=master-marker__:master-instance-only_object-a",
	)
	require.Empty(t, mockClient.RemoveOmapKeyCalls())
	require.Empty(t, mockClient.RemoveObjectCalls())
}

func configureRepairObjectIndexMocks(
	t *testing.T,
	ctx context.Context,
	mockClient *ClientMock,
	masterOLHInstance string,
	secondaryOLHInstance string,
) {
	t.Helper()

	configureRepairObjectBucketStatsMock(t, ctx, mockClient)
	configureRepairObjectShardMock(t, ctx, mockClient)
	configureRepairObjectListBucketIndexMock(t, ctx, mockClient, masterOLHInstance, secondaryOLHInstance)
}

func configureRepairObjectNoOrphanIndexMocks(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	configureRepairObjectBucketStatsMock(t, ctx, mockClient)
	configureRepairObjectShardMock(t, ctx, mockClient)

	mockClient.ListBucketIndexByObjectFunc = func(
		gotCtx context.Context,
		containerName string,
		bucketName string,
		objectName string,
		shardID int,
	) (*domain.EntryGroup, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, repairObjectBucket, bucketName)
		require.Equal(t, repairObjectName, objectName)

		switch containerName {
		case repairObjectMasterContainer:
			require.Equal(t, 7, shardID)

			return newRepairObjectMatchedEntryGroup("version-a"), nil
		case repairObjectSecondaryContainer:
			require.Equal(t, 9, shardID)

			return newRepairObjectMatchedEntryGroup("version-a"), nil
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)

			return nil, errClientFailed
		}
	}
}

func configureRepairObjectMasterOrphanIndexMocks(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	configureRepairObjectBucketStatsMock(t, ctx, mockClient)
	configureRepairObjectShardMock(t, ctx, mockClient)

	mockClient.ListBucketIndexByObjectFunc = func(
		gotCtx context.Context,
		containerName string,
		bucketName string,
		objectName string,
		shardID int,
	) (*domain.EntryGroup, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, repairObjectBucket, bucketName)
		require.Equal(t, repairObjectName, objectName)

		switch containerName {
		case repairObjectMasterContainer:
			require.Equal(t, 7, shardID)

			return newRepairObjectEntryGroup("version-a", "master-instance-only", "master-plain-only"), nil
		case repairObjectSecondaryContainer:
			require.Equal(t, 9, shardID)

			return newRepairObjectMatchedEntryGroup("version-a"), nil
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)

			return nil, errClientFailed
		}
	}
}

func configureRepairObjectBucketStatsMock(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.GetBucketStatsFunc = func(
		gotCtx context.Context,
		containerName string,
		bucketName string,
	) (*domain.BucketStats, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, repairObjectBucket, bucketName)

		switch containerName {
		case repairObjectMasterContainer:
			return domain.NewBucketStats(
				"master-bucket-id",
				repairObjectBucket,
				11,
				"master-marker",
				5,
				1,
				domain.VersioningStatusEnabled,
			)
		case repairObjectSecondaryContainer:
			return domain.NewBucketStats(
				"secondary-bucket-id",
				repairObjectBucket,
				13,
				"secondary-marker",
				5,
				1,
				domain.VersioningStatusEnabled,
			)
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)

			return nil, errClientFailed
		}
	}
}

func configureRepairObjectShardMock(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.ObjectShardFunc = func(
		gotCtx context.Context,
		containerName string,
		objectName string,
		totalShards int,
	) (*domain.ObjectShard, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, repairObjectName, objectName)

		switch containerName {
		case repairObjectMasterContainer:
			require.Equal(t, 11, totalShards)

			return domain.NewObjectShard(7), nil
		case repairObjectSecondaryContainer:
			require.Equal(t, 13, totalShards)

			return domain.NewObjectShard(9), nil
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)

			return nil, errClientFailed
		}
	}
}

func configureRepairObjectListBucketIndexMock(
	t *testing.T,
	ctx context.Context,
	mockClient *ClientMock,
	masterOLHInstance string,
	secondaryOLHInstance string,
) {
	t.Helper()

	mockClient.ListBucketIndexByObjectFunc = func(
		gotCtx context.Context,
		containerName string,
		bucketName string,
		objectName string,
		shardID int,
	) (*domain.EntryGroup, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, repairObjectBucket, bucketName)
		require.Equal(t, repairObjectName, objectName)

		switch containerName {
		case repairObjectMasterContainer:
			require.Equal(t, 7, shardID)

			return newRepairObjectEntryGroup(
				masterOLHInstance,
				"master-instance-only",
				"master-plain-only",
			), nil
		case repairObjectSecondaryContainer:
			require.Equal(t, 9, shardID)

			return newRepairObjectEntryGroup(
				secondaryOLHInstance,
				"secondary-instance-only",
				"secondary-plain-only",
			), nil
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)

			return nil, errClientFailed
		}
	}
}

func configureRepairObjectZoneMocks(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.GetDefaultZoneFunc = func(gotCtx context.Context, containerName string) (*domain.Zone, error) {
		require.Equal(t, ctx, gotCtx)

		switch containerName {
		case repairObjectMasterContainer:
			return domain.NewZone("master-data", "master-index"), nil
		case repairObjectSecondaryContainer:
			return domain.NewZone("secondary-data", "secondary-index"), nil
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)

			return nil, errClientFailed
		}
	}
}

func configureRepairObjectLayoutMocks(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.GetBucketLayoutFunc = func(
		gotCtx context.Context,
		containerName string,
		bucketName string,
	) (*domain.Layout, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, repairObjectBucket, bucketName)

		switch containerName {
		case repairObjectMasterContainer, repairObjectSecondaryContainer:
			return domain.NewLayout(1), nil
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)

			return nil, errClientFailed
		}
	}
}

func configureRepairObjectRawObjectMissingMock(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.HasRawObjectFunc = func(
		gotCtx context.Context,
		containerName string,
		pool string,
		rawObject string,
	) (bool, error) {
		require.Equal(t, ctx, gotCtx)

		switch containerName {
		case repairObjectMasterContainer:
			require.Equal(t, "master-data", pool)
			require.Contains(t, []string{
				"master-marker__:master-instance-only_" + repairObjectName,
				"master-marker__:master-plain-only_" + repairObjectName,
			}, rawObject)
		case repairObjectSecondaryContainer:
			require.Equal(t, "secondary-data", pool)
			require.Contains(t, []string{
				"secondary-marker__:secondary-instance-only_" + repairObjectName,
				"secondary-marker__:secondary-plain-only_" + repairObjectName,
			}, rawObject)
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)
		}

		return false, nil
	}
}

func configureRepairObjectRawObjectExistsMock(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.HasRawObjectFunc = func(
		gotCtx context.Context,
		containerName string,
		pool string,
		rawObject string,
	) (bool, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, repairObjectSecondaryContainer, containerName)
		require.Equal(t, "secondary-data", pool)
		require.Equal(t, "secondary-marker__:secondary-instance-only_"+repairObjectName, rawObject)

		return true, nil
	}
}

func configureRepairObjectRemoveOmapKeyMock(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.RemoveOmapKeyFunc = func(
		gotCtx context.Context,
		containerName string,
		indexPool string,
		rawObject string,
		key string,
	) error {
		require.Equal(t, ctx, gotCtx)

		switch containerName {
		case repairObjectMasterContainer:
			require.Equal(t, "master-index", indexPool)
			require.Equal(t, ".dir.master-marker.1.7", rawObject)
			require.Contains(t, []string{
				repairObjectName + ":master-instance-only",
				repairObjectName + ":master-plain-only",
			}, key)
		case repairObjectSecondaryContainer:
			require.Equal(t, "secondary-index", indexPool)
			require.Equal(t, ".dir.secondary-marker.1.9", rawObject)
			require.Contains(t, []string{
				repairObjectName + ":secondary-instance-only",
				repairObjectName + ":secondary-plain-only",
			}, key)
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)
		}

		return nil
	}
}

func configureRepairObjectMasterRawObjectExistsMock(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.HasRawObjectFunc = func(
		gotCtx context.Context,
		containerName string,
		pool string,
		rawObject string,
	) (bool, error) {
		require.Equal(t, ctx, gotCtx)

		switch containerName {
		case repairObjectMasterContainer:
			require.Equal(t, "master-data", pool)
			require.Equal(t, "master-marker__:master-instance-only_"+repairObjectName, rawObject)

			return true, nil
		case repairObjectSecondaryContainer:
			require.Equal(t, "secondary-data", pool)
			require.Contains(t, []string{
				"secondary-marker__:secondary-instance-only_" + repairObjectName,
				"secondary-marker__:secondary-plain-only_" + repairObjectName,
			}, rawObject)

			return false, nil
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)

			return false, errClientFailed
		}
	}
}

func newRepairObjectEntryGroup(
	olhInstance string,
	orphanInstance string,
	orphanPlain string,
) *domain.EntryGroup {
	return domain.NewEntryGroup(
		[]*domain.OLH{newRepairObjectOLH(olhInstance)},
		[]*domain.Plain{
			newRepairObjectPlain("matched-version"),
			newRepairObjectPlain(orphanPlain),
		},
		[]*domain.Instance{
			newRepairObjectInstance("matched-version"),
			newRepairObjectInstance(orphanInstance),
		},
	)
}

func newRepairObjectMatchedEntryGroup(olhInstance string) *domain.EntryGroup {
	return domain.NewEntryGroup(
		[]*domain.OLH{newRepairObjectOLH(olhInstance)},
		[]*domain.Plain{
			newRepairObjectPlain("matched-version"),
		},
		[]*domain.Instance{
			newRepairObjectInstance("matched-version"),
		},
	)
}

func newRepairObjectOLH(instance string) *domain.OLH {
	return domain.NewOLH(domain.OLHParams{
		DeleteMarker:   false,
		Epoch:          0,
		Exists:         true,
		Instance:       instance,
		Name:           repairObjectName,
		PendingLog:     nil,
		PendingRemoval: false,
		Tag:            "",
		IDX:            domain.NewBIIndex(repairObjectName),
	})
}

func newRepairObjectPlain(instance string) *domain.Plain {
	return domain.NewPlain(newRepairObjectDirParams(instance))
}

func newRepairObjectInstance(instance string) *domain.Instance {
	return domain.NewInstance(newRepairObjectDirParams(instance))
}

func newRepairObjectDirParams(instance string) domain.DirParams {
	return domain.DirParams{
		AccountedSize:    0,
		Appendable:       false,
		Category:         0,
		ContentType:      "",
		ETag:             "",
		Exists:           true,
		Flags:            0,
		Instance:         instance,
		Locator:          "",
		MTime:            "",
		Name:             repairObjectName,
		Owner:            "",
		OwnerDisplayName: "",
		Pending:          false,
		Size:             0,
		StorageClass:     "",
		Tag:              "",
		UserData:         "",
		Pool:             0,
		Epoch:            0,
		VersionedEpoch:   0,
		IDX:              domain.NewBIIndex(repairObjectName + ":" + instance),
	}
}
