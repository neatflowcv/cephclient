package flow_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestServiceRemoveRMSupportOmapKeysRemovesAndVerifiesEachKey(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	mockClient := newRMSupportRemovalMockClient(t, ctx)
	service := flow.NewService(mockClient)

	result, err := service.RemoveRMSupportOmapKeys(
		ctx,
		"rgw",
		"default.rgw.buckets.index",
		"bucket-marker",
		3,
		[]string{"plain-a", "plain-b"},
	)

	require.NoError(t, err)
	require.Empty(t, result.OmapKeys())
	require.Len(t, mockClient.RemoveOmapKeyCalls(), 2)
	require.Len(t, mockClient.ListOmapKeysCalls(), 2)
}

func TestServiceRemoveRMSupportOmapKeysFailsWhenKeyStillExists(t *testing.T) {
	t.Parallel()

	var mockClient ClientMock

	mockClient.RemoveOmapKeyFunc = func(context.Context, string, string, string, int, string) error {
		return nil
	}
	mockClient.ListOmapKeysFunc = func(context.Context, string, string, string, int) ([]*domain.BIIndex, error) {
		return []*domain.BIIndex{domain.NewBIIndex("plain-a")}, nil
	}

	service := flow.NewService(&mockClient)

	result, err := service.RemoveRMSupportOmapKeys(
		t.Context(),
		"rgw",
		"default.rgw.buckets.index",
		"bucket-marker",
		3,
		[]string{"plain-a"},
	)

	require.Nil(t, result)
	require.EqualError(t, err, `verify removed omap key "plain-a": omap key still exists after removal`)
}

func TestServiceRemoveRMSupportOmapKeysReturnsVerificationListError(t *testing.T) {
	t.Parallel()

	var mockClient ClientMock

	mockClient.RemoveOmapKeyFunc = func(context.Context, string, string, string, int, string) error {
		return nil
	}
	mockClient.ListOmapKeysFunc = func(context.Context, string, string, string, int) ([]*domain.BIIndex, error) {
		return nil, errClientFailed
	}

	service := flow.NewService(&mockClient)

	result, err := service.RemoveRMSupportOmapKeys(
		t.Context(),
		"rgw",
		"default.rgw.buckets.index",
		"bucket-marker",
		3,
		[]string{"plain-a"},
	)

	require.Nil(t, result)
	require.ErrorIs(t, err, errClientFailed)
	require.EqualError(t, err, `verify removed omap key "plain-a": get omap keys: client failed`)
}

func newRMSupportRemovalMockClient(t *testing.T, ctx context.Context) *ClientMock {
	t.Helper()

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
		require.Contains(t, []string{"plain-a", "plain-b"}, key)

		return nil
	}

	listCallCount := 0
	mockClient.ListOmapKeysFunc = func(
		gotCtx context.Context,
		containerName, indexPool, marker string,
		shard int,
	) ([]*domain.BIIndex, error) {
		listCallCount++

		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "rgw", containerName)
		require.Equal(t, "default.rgw.buckets.index", indexPool)
		require.Equal(t, "bucket-marker", marker)
		require.Equal(t, 3, shard)

		switch listCallCount {
		case 1:
			return []*domain.BIIndex{domain.NewBIIndex("plain-b")}, nil
		case 2:
			return []*domain.BIIndex{}, nil
		default:
			t.Fatalf("unexpected ListOmapKeys call %d", listCallCount)

			return nil, nil
		}
	}

	return &mockClient
}
