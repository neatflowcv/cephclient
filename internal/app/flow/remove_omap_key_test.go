package flow_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestServiceRemoveOmapKeyDelegatesToClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.GetBucketLayoutFunc = func(
		gotCtx context.Context,
		containerName, bucketName string,
	) (*domain.Layout, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)

		return domain.NewLayout(2), nil
	}

	mockClient.RemoveOmapKeyFunc = func(
		gotCtx context.Context,
		containerName, indexPool string,
		rawObject string,
		key string,
	) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "default.rgw.buckets.index", indexPool)
		require.Equal(t, ".dir.bucket-marker.2.3", rawObject)
		require.Equal(t, "plain-key", key)

		return nil
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveOmapKey(ctx, flow.RemoveOmapKeyRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		IndexPool:     "default.rgw.buckets.index",
		Marker:        "bucket-marker",
		ShardID:       3,
		Key:           "plain-key",
	})

	require.NoError(t, err)
	require.Len(t, mockClient.GetBucketLayoutCalls(), 1)
	require.Len(t, mockClient.RemoveOmapKeyCalls(), 1)
}

func TestServiceRemoveOmapKeyReturnsClientError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.GetBucketLayoutFunc = func(context.Context, string, string) (*domain.Layout, error) {
		return domain.NewLayout(2), nil
	}

	mockClient.RemoveOmapKeyFunc = func(context.Context, string, string, string, string) error {
		return errClientFailed
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveOmapKey(ctx, flow.RemoveOmapKeyRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		IndexPool:     "default.rgw.buckets.index",
		Marker:        "bucket-marker",
		ShardID:       3,
		Key:           "plain-key",
	})

	require.ErrorIs(t, err, errClientFailed)
	require.EqualError(t, err, "remove omap key: client failed")
	require.Len(t, mockClient.GetBucketLayoutCalls(), 1)
	require.Len(t, mockClient.RemoveOmapKeyCalls(), 1)
}

func TestServiceRemoveOmapKeyReturnsBucketLayoutError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.GetBucketLayoutFunc = func(context.Context, string, string) (*domain.Layout, error) {
		return nil, errClientFailed
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveOmapKey(ctx, flow.RemoveOmapKeyRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		IndexPool:     "default.rgw.buckets.index",
		Marker:        "bucket-marker",
		ShardID:       3,
		Key:           "plain-key",
	})

	require.ErrorIs(t, err, errClientFailed)
	require.EqualError(t, err, "get bucket layout: client failed")
	require.Len(t, mockClient.GetBucketLayoutCalls(), 1)
	require.Empty(t, mockClient.RemoveOmapKeyCalls())
}
