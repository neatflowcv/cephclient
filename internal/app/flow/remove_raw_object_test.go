package flow_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/stretchr/testify/require"
)

func TestServiceRemoveRawObjectDelegatesToClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.RemoveRawObjectFunc = func(
		gotCtx context.Context,
		containerName, pool, rawObject string,
	) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "default.rgw.buckets.data", pool)
		require.Equal(t, "marker__:test-object", rawObject)

		return nil
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveRawObject(ctx, "rgw", "default.rgw.buckets.data", "marker__:test-object")

	require.NoError(t, err)
	require.Len(t, mockClient.RemoveRawObjectCalls(), 1)
}

func TestServiceRemoveRawObjectReturnsClientError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.RemoveRawObjectFunc = func(context.Context, string, string, string) error {
		return errClientFailed
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveRawObject(ctx, "rgw", "default.rgw.buckets.data", "marker__:test-object")

	require.ErrorIs(t, err, errClientFailed)
	require.EqualError(t, err, "remove raw object: client failed")
	require.Len(t, mockClient.RemoveRawObjectCalls(), 1)
}
