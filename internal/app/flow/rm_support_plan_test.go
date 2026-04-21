package flow_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestServiceRMSupportPlanDelegatesWithoutOmap(t *testing.T) { //nolint:funlen
	t.Parallel()

	ctx := t.Context()
	stats := mustBucketStats(t, "bucket-id", 11, "bucket-marker")
	wantList := newFlowBIList()

	var mockClient ClientMock

	mockClient.BucketStatsFunc = func(
		gotCtx context.Context,
		containerName, bucketName string,
	) (*domain.BucketStats, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)

		return stats, nil
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

		return domain.NewObjectShard(3), nil
	}
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
	mockClient.GetDefaultZoneFunc = func(gotCtx context.Context, containerName string) (*domain.Zone, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)

		return domain.NewZone("data-pool", "index-pool"), nil
	}
	service := flow.NewService(&mockClient)

	plan, err := service.BuildRMSupportPlan(ctx, "rgw", "bucket-a", "test.txt", false)

	require.NoError(t, err)
	require.Same(t, wantList, plan.BIList())
	require.Equal(t, 3, plan.ShardID())
	require.Equal(t, "bucket-marker", plan.Marker())
	require.Equal(t, "index-pool", plan.IndexPool())
	require.Empty(t, plan.OmapKeys())
	require.Len(t, mockClient.BucketStatsCalls(), 1)
	require.Len(t, mockClient.ObjectShardCalls(), 1)
	require.Len(t, mockClient.BIListByObjectCalls(), 1)
	require.Len(t, mockClient.GetDefaultZoneCalls(), 1)
	require.Empty(t, mockClient.ListOmapKeysCalls())
}

func TestServiceRMSupportPlanDelegatesWithOmap(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	stats := mustBucketStats(t, "bucket-id", 11, "bucket-marker")

	wantList := domain.NewBIList([]domain.BIEntry{})
	wantOmapKeys := []*domain.BIIndex{
		domain.NewBIIndex("plain"),
		domain.NewBIIndex("versioned"),
	}

	var mockClient ClientMock

	mockClient.BucketStatsFunc = func(context.Context, string, string) (*domain.BucketStats, error) {
		return stats, nil
	}
	mockClient.ObjectShardFunc = func(context.Context, string, string, int) (*domain.ObjectShard, error) {
		return domain.NewObjectShard(3), nil
	}
	mockClient.BIListByObjectFunc = func(context.Context, string, string, string, int) (*domain.BIList, error) {
		return wantList, nil
	}
	mockClient.GetDefaultZoneFunc = func(gotCtx context.Context, containerName string) (*domain.Zone, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)

		return domain.NewZone("data-pool", "index-pool"), nil
	}
	mockClient.ListOmapKeysFunc = func(
		gotCtx context.Context,
		containerName, indexPool, marker string,
		shard int,
	) ([]*domain.BIIndex, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "index-pool", indexPool)
		require.Equal(t, "bucket-marker", marker)
		require.Equal(t, 3, shard)

		return wantOmapKeys, nil
	}
	service := flow.NewService(&mockClient)

	plan, err := service.BuildRMSupportPlan(ctx, "rgw", "bucket-a", "test.txt", true)

	require.NoError(t, err)
	require.Same(t, wantList, plan.BIList())
	require.Equal(t, "index-pool", plan.IndexPool())
	require.Equal(t, wantOmapKeys, plan.OmapKeys())
	require.Len(t, mockClient.GetDefaultZoneCalls(), 1)
	require.Len(t, mockClient.ListOmapKeysCalls(), 1)
}

func mustBucketStats(t *testing.T, bucketID string, totalShards int, marker string) *domain.BucketStats {
	t.Helper()

	stats, err := domain.NewBucketStats(bucketID, "test", totalShards, marker, 5, 1, domain.VersioningStatusEnabled)
	require.NoError(t, err)

	return stats
}

func newFlowBIList() *domain.BIList {
	return domain.NewBIList([]domain.BIEntry{
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
}
