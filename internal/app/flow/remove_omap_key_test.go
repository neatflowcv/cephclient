package flow_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/stretchr/testify/require"
)

func TestServiceRemoveOmapKeyDelegatesToClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.RemoveOmapKeyFunc = func(
		gotCtx context.Context,
		containerName, indexPool, marker string,
		shard int,
		key string,
	) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "default.rgw.buckets.index", indexPool)
		require.Equal(t, "bucket-marker", marker)
		require.Equal(t, 3, shard)
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

	mockClient.RemoveOmapKeyFunc = func(context.Context, string, string, string, int, string) error {
		return errClientFailed
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveOmapKey(ctx, "rgw", "default.rgw.buckets.index", "bucket-marker", 3, "plain-key")

	require.ErrorIs(t, err, errClientFailed)
	require.EqualError(t, err, "remove omap key: client failed")
	require.Len(t, mockClient.RemoveOmapKeyCalls(), 1)
}
