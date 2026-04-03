package flow_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/stretchr/testify/require"
)

func TestServiceRemoveObjectDelegatesToClient(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.RemoveObjectFunc = func(
		gotCtx context.Context,
		containerName, bucketName, objectName, version string,
	) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "test-bucket", bucketName)
		require.Equal(t, "test-object", objectName)
		require.Equal(t, "version-1", version)

		return nil
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveObject(ctx, "rgw", "test-bucket", "test-object", "version-1")

	require.NoError(t, err)
	require.Len(t, mockClient.RemoveObjectCalls(), 1)
}

func TestServiceRemoveObjectReturnsClientError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.RemoveObjectFunc = func(context.Context, string, string, string, string) error {
		return errClientFailed
	}
	service := flow.NewService(&mockClient)

	err := service.RemoveObject(ctx, "rgw", "test-bucket", "test-object", "version-1")

	require.ErrorIs(t, err, errClientFailed)
	require.EqualError(t, err, "remove object: client failed")
	require.Len(t, mockClient.RemoveObjectCalls(), 1)
}
