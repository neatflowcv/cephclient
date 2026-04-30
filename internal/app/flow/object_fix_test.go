package flow_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

const (
	fixObjectTargetContainer    = "rgw-a"
	fixObjectReferenceContainer = "rgw-b"
	fixObjectBucket             = "bucket-a"
	fixObjectName               = "object-a"
)

func TestServiceFixObjectReadsTargetAndReferenceBucketIndex(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	configureFixObjectIndexMocks(t, ctx, &mockClient)

	service := flow.NewService(&mockClient)

	err := service.FixObject(ctx, flow.FixObjectRequest{
		TargetContainerName:    fixObjectTargetContainer,
		ReferenceContainerName: fixObjectReferenceContainer,
		BucketName:             fixObjectBucket,
		ObjectName:             fixObjectName,
	})

	require.NoError(t, err)
	require.Len(t, mockClient.GetBucketStatsCalls(), 2)
	require.Len(t, mockClient.ObjectShardCalls(), 2)

	listCalls := mockClient.ListBucketIndexByObjectCalls()
	require.Len(t, listCalls, 3)
	require.Equal(t, fixObjectTargetContainer, listCalls[0].ContainerName)
	require.Equal(t, fixObjectReferenceContainer, listCalls[1].ContainerName)
	require.Equal(t, fixObjectTargetContainer, listCalls[2].ContainerName)
}

func TestServiceFixObjectReturnsErrorWhenOLHInstancesDiffer(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	configureFixObjectBucketStatsMock(t, ctx, &mockClient)
	configureFixObjectShardMock(t, ctx, &mockClient)
	configureFixObjectListBucketIndexMock(t, ctx, &mockClient, "target-version", "reference-version")

	service := flow.NewService(&mockClient)

	err := service.FixObject(ctx, flow.FixObjectRequest{
		TargetContainerName:    fixObjectTargetContainer,
		ReferenceContainerName: fixObjectReferenceContainer,
		BucketName:             fixObjectBucket,
		ObjectName:             fixObjectName,
	})

	require.EqualError(
		t,
		err,
		"object olh instance mismatch: target=[target-version] reference=[reference-version]",
	)
}

func TestServiceFixObjectRemovesTargetInstancesWithoutPlain(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	configureFixObjectBucketStatsMock(t, ctx, &mockClient)
	configureFixObjectShardMock(t, ctx, &mockClient)
	configureFixObjectListBucketIndexMock(t, ctx, &mockClient, "version-a", "version-a")
	configureFixObjectRemoveObjectMock(t, ctx, &mockClient)

	service := flow.NewService(&mockClient)

	err := service.FixObject(ctx, flow.FixObjectRequest{
		TargetContainerName:    fixObjectTargetContainer,
		ReferenceContainerName: fixObjectReferenceContainer,
		BucketName:             fixObjectBucket,
		ObjectName:             fixObjectName,
	})

	require.NoError(t, err)

	removeCalls := mockClient.RemoveObjectCalls()
	require.Len(t, removeCalls, 1)
	require.Equal(t, fixObjectTargetContainer, removeCalls[0].ContainerName)
	require.Equal(t, fixObjectBucket, removeCalls[0].BucketName)
	require.Equal(t, fixObjectName, removeCalls[0].ObjectName)
	require.Equal(t, "missing-plain", removeCalls[0].Version)
}

func TestServiceFixObjectContinuesWhenRemoveObjectFails(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	configureFixObjectBucketStatsMock(t, ctx, &mockClient)
	configureFixObjectShardMock(t, ctx, &mockClient)
	configureFixObjectListBucketIndexMock(t, ctx, &mockClient, "version-a", "version-a")
	configureFixObjectRemoveObjectErrorMock(t, ctx, &mockClient)

	service := flow.NewService(&mockClient)

	err := service.FixObject(ctx, flow.FixObjectRequest{
		TargetContainerName:    fixObjectTargetContainer,
		ReferenceContainerName: fixObjectReferenceContainer,
		BucketName:             fixObjectBucket,
		ObjectName:             fixObjectName,
	})

	require.NoError(t, err)
	require.Len(t, mockClient.RemoveObjectCalls(), 1)
}

func TestServiceFixObjectRemovesRawObjectAndOmapKeyWhenObjectStillRemains(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	configureFixObjectBucketStatsMock(t, ctx, &mockClient)
	configureFixObjectShardMock(t, ctx, &mockClient)
	configureFixObjectRemainingBucketIndexMock(t, ctx, &mockClient)
	configureFixObjectRemoveObjectMock(t, ctx, &mockClient)
	configureFixObjectFallbackMocks(t, ctx, &mockClient)

	service := flow.NewService(&mockClient)

	err := service.FixObject(ctx, flow.FixObjectRequest{
		TargetContainerName:    fixObjectTargetContainer,
		ReferenceContainerName: fixObjectReferenceContainer,
		BucketName:             fixObjectBucket,
		ObjectName:             fixObjectName,
	})

	require.NoError(t, err)

	rawCalls := mockClient.RemoveRawObjectCalls()
	require.Len(t, rawCalls, 1)
	require.Equal(t, "default.rgw.buckets.data", rawCalls[0].Pool)
	require.Equal(t, "bucket-marker-a__:missing-plain_"+fixObjectName, rawCalls[0].RawObject)

	omapCalls := mockClient.RemoveOmapKeyCalls()
	require.Len(t, omapCalls, 1)
	require.Equal(t, "default.rgw.buckets.index", omapCalls[0].IndexPool)
	require.Equal(t, ".dir.bucket-marker-a.1.7", omapCalls[0].RawObject)
	require.Equal(t, fixObjectName+":missing-plain", omapCalls[0].Key)
}

func configureFixObjectIndexMocks(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	configureFixObjectBucketStatsMock(t, ctx, mockClient)
	configureFixObjectShardMock(t, ctx, mockClient)
	configureFixObjectListBucketIndexMock(t, ctx, mockClient, "version-a", "version-a")
	configureFixObjectRemoveObjectMock(t, ctx, mockClient)
}

func configureFixObjectBucketStatsMock(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.GetBucketStatsFunc = func(
		gotCtx context.Context,
		containerName, bucketName string,
	) (*domain.BucketStats, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, fixObjectBucket, bucketName)

		switch containerName {
		case fixObjectTargetContainer:
			return domain.NewBucketStats(
				"bucket-id-a",
				fixObjectBucket,
				11,
				"bucket-marker-a",
				5,
				1,
				domain.VersioningStatusEnabled,
			)
		case fixObjectReferenceContainer:
			return domain.NewBucketStats(
				"bucket-id-b",
				fixObjectBucket,
				13,
				"bucket-marker-b",
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

func configureFixObjectShardMock(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.ObjectShardFunc = func(
		gotCtx context.Context,
		containerName, objectName string,
		totalShards int,
	) (*domain.ObjectShard, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, fixObjectName, objectName)

		switch containerName {
		case fixObjectTargetContainer:
			require.Equal(t, 11, totalShards)

			return domain.NewObjectShard(7), nil
		case fixObjectReferenceContainer:
			require.Equal(t, 13, totalShards)

			return domain.NewObjectShard(9), nil
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)

			return nil, errClientFailed
		}
	}
}

func configureFixObjectListBucketIndexMock(
	t *testing.T,
	ctx context.Context,
	mockClient *ClientMock,
	targetInstance,
	referenceInstance string,
) {
	t.Helper()

	targetCallCount := 0

	mockClient.ListBucketIndexByObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName string,
		shardID int,
	) (*domain.EntryGroup, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, fixObjectBucket, bucketName)
		require.Equal(t, fixObjectName, objectName)

		switch containerName {
		case fixObjectTargetContainer:
			require.Equal(t, 7, shardID)

			targetCallCount++
			if targetCallCount > 1 {
				return domain.NewEntryGroup(nil, nil, nil), nil
			}

			return newFixObjectTargetEntryGroup(targetInstance), nil
		case fixObjectReferenceContainer:
			require.Equal(t, 9, shardID)

			return newFixObjectReferenceEntryGroup(referenceInstance), nil
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)
		}

		return nil, errClientFailed
	}
}

func configureFixObjectRemainingBucketIndexMock(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.ListBucketIndexByObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName string,
		shardID int,
	) (*domain.EntryGroup, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, fixObjectBucket, bucketName)
		require.Equal(t, fixObjectName, objectName)

		switch containerName {
		case fixObjectTargetContainer:
			require.Equal(t, 7, shardID)

			return newFixObjectTargetEntryGroup("version-a"), nil
		case fixObjectReferenceContainer:
			require.Equal(t, 9, shardID)

			return newFixObjectReferenceEntryGroup("version-a"), nil
		default:
			require.FailNow(t, "unexpected container name", "container=%s", containerName)
		}

		return nil, errClientFailed
	}
}

func configureFixObjectRemoveObjectMock(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.RemoveObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName, version string,
	) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, fixObjectTargetContainer, containerName)
		require.Equal(t, fixObjectBucket, bucketName)
		require.Equal(t, fixObjectName, objectName)
		require.Equal(t, "missing-plain", version)

		return nil
	}
}

func configureFixObjectRemoveObjectErrorMock(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.RemoveObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName, version string,
	) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, fixObjectTargetContainer, containerName)
		require.Equal(t, fixObjectBucket, bucketName)
		require.Equal(t, fixObjectName, objectName)
		require.Equal(t, "missing-plain", version)

		return errClientFailed
	}
}

func configureFixObjectFallbackMocks(t *testing.T, ctx context.Context, mockClient *ClientMock) {
	t.Helper()

	mockClient.GetDefaultZoneFunc = func(gotCtx context.Context, containerName string) (*domain.Zone, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, fixObjectTargetContainer, containerName)

		return domain.NewZone("default.rgw.buckets.data", "default.rgw.buckets.index"), nil
	}
	mockClient.GetBucketLayoutFunc = func(
		gotCtx context.Context,
		containerName, bucketName string,
	) (*domain.Layout, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, fixObjectTargetContainer, containerName)
		require.Equal(t, fixObjectBucket, bucketName)

		return domain.NewLayout(1), nil
	}
	mockClient.RemoveRawObjectFunc = func(gotCtx context.Context, containerName, pool, rawObject string) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, fixObjectTargetContainer, containerName)
		require.Equal(t, "default.rgw.buckets.data", pool)
		require.Equal(t, "bucket-marker-a__:missing-plain_"+fixObjectName, rawObject)

		return nil
	}
	mockClient.RemoveOmapKeyFunc = func(
		gotCtx context.Context,
		containerName, indexPool string,
		rawObject string,
		key string,
	) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, fixObjectTargetContainer, containerName)
		require.Equal(t, "default.rgw.buckets.index", indexPool)
		require.Equal(t, ".dir.bucket-marker-a.1.7", rawObject)
		require.Equal(t, fixObjectName+":missing-plain", key)

		return nil
	}
}

func newFixObjectTargetEntryGroup(olhInstance string) *domain.EntryGroup {
	return domain.NewEntryGroup(
		[]*domain.OLH{newFixObjectOLH(olhInstance)},
		[]*domain.Plain{
			newFixObjectPlain("matched-plain"),
		},
		[]*domain.Instance{
			newFixObjectInstance("matched-plain"),
			newFixObjectInstance("missing-plain"),
		},
	)
}

func newFixObjectReferenceEntryGroup(olhInstance string) *domain.EntryGroup {
	return domain.NewEntryGroup(
		[]*domain.OLH{newFixObjectOLH(olhInstance)},
		nil,
		nil,
	)
}

func newFixObjectOLH(instance string) *domain.OLH {
	return domain.NewOLH(domain.OLHParams{
		DeleteMarker:   false,
		Epoch:          0,
		Exists:         true,
		Instance:       instance,
		Name:           fixObjectName,
		PendingLog:     nil,
		PendingRemoval: false,
		Tag:            "",
		IDX:            domain.NewBIIndex(fixObjectName),
	})
}

func newFixObjectPlain(instance string) *domain.Plain {
	return domain.NewPlain(newFixObjectDirParams(instance))
}

func newFixObjectInstance(instance string) *domain.Instance {
	return domain.NewInstance(newFixObjectDirParams(instance))
}

func newFixObjectDirParams(instance string) domain.DirParams {
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
		Name:             fixObjectName,
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
		IDX:              domain.NewBIIndex(fixObjectName + ":" + instance),
	}
}
