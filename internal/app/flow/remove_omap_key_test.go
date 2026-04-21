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
		require.Equal(t, 3, indexObject.Shard())
		require.Equal(t, "plain-key", key)

		return nil
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveOmapKey(ctx, "rgw", "default.rgw.buckets.index", "bucket-marker", 3, "plain-key")

	require.NoError(t, err)
	require.Len(t, mockClient.RemoveOmapKeyCalls(), 1)
}

func TestServiceRemoveOmapKeyReturnsClientError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.RemoveOmapKeyFunc = func(context.Context, string, string, *domain.BucketIndexObject, string) error {
		return errClientFailed
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveOmapKey(ctx, "rgw", "default.rgw.buckets.index", "bucket-marker", 3, "plain-key")

	require.ErrorIs(t, err, errClientFailed)
	require.EqualError(t, err, "remove omap key: client failed")
	require.Len(t, mockClient.RemoveOmapKeyCalls(), 1)
}
