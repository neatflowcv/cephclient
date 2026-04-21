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
		containerName, indexPool string,
		indexObject *domain.BucketIndexObject,
	) ([]*domain.BIIndex, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "default.rgw.buckets.index", indexPool)
		require.Equal(t, "bucket-marker", indexObject.Marker())
		require.Equal(t, 3, indexObject.Shard())

		return wantIndexes, nil
	}
	service := flow.NewService(&mockClient)

	// Act
	resp, err := service.ListOmapKeys(ctx, flow.ListOmapKeysRequest{
		ContainerName: "rgw",
		IndexPool:     "default.rgw.buckets.index",
		Marker:        "bucket-marker",
		ShardID:       3,
	})

	// Assert
	require.NoError(t, err)
	require.Equal(t, []string{"plain", "versioned"}, resp.OmapKeys)
	require.Len(t, mockClient.ListOmapKeysCalls(), 1)
}

func TestServiceListOmapKeysReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.ListOmapKeysFunc = func(
		context.Context,
		string,
		string,
		*domain.BucketIndexObject,
	) ([]*domain.BIIndex, error) {
		return nil, wantErr
	}
	service := flow.NewService(&mockClient)

	// Act
	_, err := service.ListOmapKeys(ctx, flow.ListOmapKeysRequest{
		ContainerName: "rgw",
		IndexPool:     "default.rgw.buckets.index",
		Marker:        "bucket-marker",
		ShardID:       3,
	})

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.EqualError(t, err, "get omap keys: client failed")
	require.Len(t, mockClient.ListOmapKeysCalls(), 1)
}
