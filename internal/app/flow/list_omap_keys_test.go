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

	mockClient.BucketLayoutFunc = func(gotCtx context.Context, containerName, bucketName string) (*domain.Layout, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "bucket-a", bucketName)

		return domain.NewLayout(2), nil
	}

	mockClient.ListOmapKeysFunc = func(
		gotCtx context.Context,
		containerName, indexPool string,
		rawObject string,
	) ([]*domain.BIIndex, error) {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "default.rgw.buckets.index", indexPool)
		require.Equal(t, ".dir.bucket-marker.2.3", rawObject)

		return wantIndexes, nil
	}
	service := flow.NewService(&mockClient)

	// Act
	resp, err := service.ListOmapKeys(ctx, flow.ListOmapKeysRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		IndexPool:     "default.rgw.buckets.index",
		Marker:        "bucket-marker",
		ShardID:       3,
	})

	// Assert
	require.NoError(t, err)
	require.Equal(t, []string{"plain", "versioned"}, resp.OmapKeys)
	require.Len(t, mockClient.BucketLayoutCalls(), 1)
	require.Len(t, mockClient.ListOmapKeysCalls(), 1)
}

func TestServiceListOmapKeysReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	wantErr := errClientFailed

	var mockClient ClientMock

	mockClient.BucketLayoutFunc = func(context.Context, string, string) (*domain.Layout, error) {
		return domain.NewLayout(2), nil
	}

	mockClient.ListOmapKeysFunc = func(
		context.Context,
		string,
		string,
		string,
	) ([]*domain.BIIndex, error) {
		return nil, wantErr
	}
	service := flow.NewService(&mockClient)

	// Act
	_, err := service.ListOmapKeys(ctx, flow.ListOmapKeysRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		IndexPool:     "default.rgw.buckets.index",
		Marker:        "bucket-marker",
		ShardID:       3,
	})

	// Assert
	require.ErrorIs(t, err, wantErr)
	require.EqualError(t, err, "get omap keys: client failed")
	require.Len(t, mockClient.BucketLayoutCalls(), 1)
	require.Len(t, mockClient.ListOmapKeysCalls(), 1)
}

func TestServiceListOmapKeysReturnsBucketLayoutError(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var mockClient ClientMock

	mockClient.BucketLayoutFunc = func(context.Context, string, string) (*domain.Layout, error) {
		return nil, errClientFailed
	}
	service := flow.NewService(&mockClient)

	_, err := service.ListOmapKeys(ctx, flow.ListOmapKeysRequest{
		ContainerName: "rgw",
		BucketName:    "bucket-a",
		IndexPool:     "default.rgw.buckets.index",
		Marker:        "bucket-marker",
		ShardID:       3,
	})

	require.ErrorIs(t, err, errClientFailed)
	require.EqualError(t, err, "get bucket layout: client failed")
	require.Len(t, mockClient.BucketLayoutCalls(), 1)
	require.Empty(t, mockClient.ListOmapKeysCalls())
}
