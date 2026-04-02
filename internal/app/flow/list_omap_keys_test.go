package flow_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestServiceListOmapKeysDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantIndexes := []*domain.BIIndex{
		domain.NewBIIndex("plain"),
		domain.NewBIIndex("versioned"),
	}

	var mockClient ClientMock

	mockClient.ListOmapKeysFunc = func(
		gotCtx context.Context,
		containerName, indexPool, marker string,
		shard int,
	) ([]*domain.BIIndex, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "default.rgw.buckets.index", indexPool)
		require.Equal(t, "bucket-marker", marker)
		require.Equal(t, 3, shard)

		return wantIndexes, nil
	}
	service := flow.NewService(&mockClient)

	// Act
	indexes, err := service.ListOmapKeys(ctx, "rgw", "default.rgw.buckets.index", "bucket-marker", 3)

	// Assert
	require.NoError(t, err)
	require.Equal(t, wantIndexes, indexes)
	require.Len(t, mockClient.ListOmapKeysCalls(), 1)
}

func TestServiceListOmapKeysReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.ListOmapKeysFunc = func(context.Context, string, string, string, int) ([]*domain.BIIndex, error) {
		return nil, wantErr
	}
	service := flow.NewService(&mockClient)

	// Act
	_, err := service.ListOmapKeys(ctx, "rgw", "default.rgw.buckets.index", "bucket-marker", 3)

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.Len(t, mockClient.ListOmapKeysCalls(), 1)
}
