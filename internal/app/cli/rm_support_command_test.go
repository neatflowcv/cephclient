package cli //nolint:testpackage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestRMSupportCommandRunRemovesSelectedEntriesAndShowsBeforeAfterOmap(t *testing.T) { //nolint:funlen
	t.Parallel()

	var client stubFlowClient

	client.BucketStatsFunc = func(context.Context, string, string) (*domain.BucketStats, error) {
		return mustCLIBucketStats(t, "bucket-id", 11, "bucket-marker"), nil
	}
	client.ObjectShardFunc = func(context.Context, string, string, int) (*domain.ObjectShard, error) {
		return domain.NewObjectShard(3), nil
	}
	client.BIListByObjectFunc = func(context.Context, string, string, string, int) (*domain.BIList, error) {
		return newCLIBIList(), nil
	}
	client.GetDefaultZoneFunc = func(context.Context, string) (*domain.Zone, error) {
		return domain.NewZone("data-pool", "index-pool"), nil
	}

	listCallCount := 0
	client.ListOmapKeysFunc = func(context.Context, string, string, string, int) ([]*domain.BIIndex, error) {
		listCallCount++

		switch listCallCount {
		case 1:
			return []*domain.BIIndex{
				domain.NewBIIndex("test.txt"),
				domain.NewBIIndex("other.txt"),
			}, nil
		case 2:
			return []*domain.BIIndex{domain.NewBIIndex("other.txt")}, nil
		default:
			t.Fatalf("unexpected ListOmapKeys call %d", listCallCount)

			return nil, nil
		}
	}

	client.RemoveOmapKeyFunc = func(context.Context, string, string, string, int, string) error {
		return nil
	}

	service := flow.NewService(&client)
	command := rmSupportCommand{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
		ShowOmap:      true,
	}

	var stdout strings.Builder

	stdin := strings.NewReader("1\nyes\n")

	err := command.Run(t.Context(), service, stdin, &stdout)

	require.NoError(t, err)
	require.Len(t, client.RemoveOmapKeyCalls, 1)
	require.Equal(t, "test.txt", client.RemoveOmapKeyCalls[0].Key)
	require.Contains(t, stdout.String(), "omap keys before removal: index_pool=index-pool marker=bucket-marker shard=3")
	require.Contains(t, stdout.String(), "omap keys after removal: index_pool=index-pool marker=bucket-marker shard=3")
	require.Contains(t, stdout.String(), `idx="test.txt"`)
	require.Contains(t, stdout.String(), `idx="other.txt"`)
}

func TestRMSupportCommandRunDoesNotRemoveWhenCancelled(t *testing.T) {
	t.Parallel()

	var client stubFlowClient

	client.BucketStatsFunc = func(context.Context, string, string) (*domain.BucketStats, error) {
		return mustCLIBucketStats(t, "bucket-id", 11, "bucket-marker"), nil
	}
	client.ObjectShardFunc = func(context.Context, string, string, int) (*domain.ObjectShard, error) {
		return domain.NewObjectShard(3), nil
	}
	client.BIListByObjectFunc = func(context.Context, string, string, string, int) (*domain.BIList, error) {
		return newCLIBIList(), nil
	}
	client.GetDefaultZoneFunc = func(context.Context, string) (*domain.Zone, error) {
		return domain.NewZone("data-pool", "index-pool"), nil
	}

	service := flow.NewService(&client)
	command := rmSupportCommand{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
		ShowOmap:      false,
	}

	var stdout strings.Builder

	stdin := strings.NewReader("1\nno\n")

	err := command.Run(t.Context(), service, stdin, &stdout)

	require.NoError(t, err)
	require.Empty(t, client.RemoveOmapKeyCalls)
	require.Contains(t, stdout.String(), "rm-support cancelled.")
}

func TestRMSupportCommandRunReturnsRemovalVerificationError(t *testing.T) {
	t.Parallel()

	var client stubFlowClient

	client.BucketStatsFunc = func(context.Context, string, string) (*domain.BucketStats, error) {
		return mustCLIBucketStats(t, "bucket-id", 11, "bucket-marker"), nil
	}
	client.ObjectShardFunc = func(context.Context, string, string, int) (*domain.ObjectShard, error) {
		return domain.NewObjectShard(3), nil
	}
	client.BIListByObjectFunc = func(context.Context, string, string, string, int) (*domain.BIList, error) {
		return newCLIBIList(), nil
	}
	client.GetDefaultZoneFunc = func(context.Context, string) (*domain.Zone, error) {
		return domain.NewZone("data-pool", "index-pool"), nil
	}
	client.RemoveOmapKeyFunc = func(context.Context, string, string, string, int, string) error {
		return nil
	}
	client.ListOmapKeysFunc = func(context.Context, string, string, string, int) ([]*domain.BIIndex, error) {
		return []*domain.BIIndex{domain.NewBIIndex("test.txt")}, nil
	}

	service := flow.NewService(&client)
	command := rmSupportCommand{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		ObjectName:    "test.txt",
		ShowOmap:      false,
	}

	err := command.Run(t.Context(), service, strings.NewReader("1\nyes\n"), &strings.Builder{})

	require.EqualError(
		t,
		err,
		`execute rm-support removal: verify removed omap key "test.txt": omap key still exists after removal`,
	)
}

type stubFlowClient struct {
	BIListByObjectFunc func(context.Context, string, string, string, int) (*domain.BIList, error)
	BucketLayoutFunc   func(context.Context, string, string) (*domain.Layout, error)
	BucketStatsFunc    func(context.Context, string, string) (*domain.BucketStats, error)
	GetDefaultZoneFunc func(context.Context, string) (*domain.Zone, error)
	ListBucketsFunc    func(context.Context, string) ([]string, error)
	ListOmapKeysFunc   func(context.Context, string, string, string, int) ([]*domain.BIIndex, error)
	ObjectShardFunc    func(context.Context, string, string, int) (*domain.ObjectShard, error)
	RemoveOmapKeyFunc  func(context.Context, string, string, string, int, string) error

	RemoveOmapKeyCalls []stubRemoveOmapKeyCall
}

type stubRemoveOmapKeyCall struct {
	ContainerName string
	IndexPool     string
	Marker        string
	Shard         int
	Key           string
}

var errUnexpectedBIListByShardCall = errors.New("unexpected BIListByShard call")

func (s *stubFlowClient) BIListByShard(context.Context, string, string, int) (*domain.BIList, error) {
	return nil, errUnexpectedBIListByShardCall
}

func (s *stubFlowClient) BIListByObject(
	ctx context.Context,
	containerName, bucketName, objectName string,
	shardID int,
) (*domain.BIList, error) {
	return s.BIListByObjectFunc(ctx, containerName, bucketName, objectName, shardID)
}

func (s *stubFlowClient) BucketLayout(ctx context.Context, containerName, bucketName string) (*domain.Layout, error) {
	return s.BucketLayoutFunc(ctx, containerName, bucketName)
}

func (s *stubFlowClient) BucketStats(
	ctx context.Context,
	containerName, bucketName string,
) (*domain.BucketStats, error) {
	return s.BucketStatsFunc(ctx, containerName, bucketName)
}

func (s *stubFlowClient) GetDefaultZone(ctx context.Context, containerName string) (*domain.Zone, error) {
	return s.GetDefaultZoneFunc(ctx, containerName)
}

func (s *stubFlowClient) ListOmapKeys(
	ctx context.Context,
	containerName, indexPool, marker string,
	shard int,
) ([]*domain.BIIndex, error) {
	return s.ListOmapKeysFunc(ctx, containerName, indexPool, marker, shard)
}

func (s *stubFlowClient) ListBuckets(ctx context.Context, containerName string) ([]string, error) {
	return s.ListBucketsFunc(ctx, containerName)
}

func (s *stubFlowClient) ObjectShard(
	ctx context.Context,
	containerName, objectName string,
	totalShards int,
) (*domain.ObjectShard, error) {
	return s.ObjectShardFunc(ctx, containerName, objectName, totalShards)
}

func (s *stubFlowClient) RemoveOmapKey(
	ctx context.Context,
	containerName, indexPool, marker string,
	shard int,
	key string,
) error {
	s.RemoveOmapKeyCalls = append(s.RemoveOmapKeyCalls, stubRemoveOmapKeyCall{
		ContainerName: containerName,
		IndexPool:     indexPool,
		Marker:        marker,
		Shard:         shard,
		Key:           key,
	})

	return s.RemoveOmapKeyFunc(ctx, containerName, indexPool, marker, shard, key)
}

func mustCLIBucketStats(t *testing.T, bucketID string, totalShards int, marker string) *domain.BucketStats {
	t.Helper()

	stats, err := domain.NewBucketStats(bucketID, totalShards, marker, domain.VersioningStatusEnabled)
	require.NoError(t, err)

	return stats
}

func newCLIBIList() *domain.BIList {
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
				nil,
				0,
			),
		),
	})
}
