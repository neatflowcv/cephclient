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

	mockClient.BucketLayoutFunc = func(gotCtx context.Context, containerName, bucketName string) (*domain.Layout, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)

		return domain.NewLayout(2), nil
	}

	mockClient.RemoveOmapKeyFunc = func(
		gotCtx context.Context,
		containerName, indexPool string,
		indexObject *domain.BucketIndexObject,
		key string,
	) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "default.rgw.buckets.index", indexPool)
		require.Equal(t, "bucket-marker", indexObject.Marker())
		require.Equal(t, 2, indexObject.Layout())
		require.Equal(t, 3, indexObject.Shard())
		require.Equal(t, "plain-key", key)

		return nil
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveOmapKey(ctx, "rgw", "bucket-a", "default.rgw.buckets.index", "bucket-marker", 3, "plain-key")

	require.NoError(t, err)
	require.Len(t, mockClient.BucketLayoutCalls(), 1)
	require.Len(t, mockClient.RemoveOmapKeyCalls(), 1)
}

func TestServiceRemoveOmapKeyReturnsClientError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.BucketLayoutFunc = func(context.Context, string, string) (*domain.Layout, error) {
		return domain.NewLayout(2), nil
	}

	mockClient.RemoveOmapKeyFunc = func(context.Context, string, string, *domain.BucketIndexObject, string) error {
		return errClientFailed
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveOmapKey(ctx, "rgw", "bucket-a", "default.rgw.buckets.index", "bucket-marker", 3, "plain-key")

	require.ErrorIs(t, err, errClientFailed)
	require.EqualError(t, err, "remove omap key: client failed")
	require.Len(t, mockClient.BucketLayoutCalls(), 1)
	require.Len(t, mockClient.RemoveOmapKeyCalls(), 1)
}

func TestServiceRemoveOmapKeyReturnsBucketLayoutError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.BucketLayoutFunc = func(context.Context, string, string) (*domain.Layout, error) {
		return nil, errClientFailed
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveOmapKey(ctx, "rgw", "bucket-a", "default.rgw.buckets.index", "bucket-marker", 3, "plain-key")

	require.ErrorIs(t, err, errClientFailed)
	require.EqualError(t, err, "get bucket layout: client failed")
	require.Len(t, mockClient.BucketLayoutCalls(), 1)
	require.Empty(t, mockClient.RemoveOmapKeyCalls())
}
