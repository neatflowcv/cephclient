package flow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/client"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestServiceBucketStatsDelegatesToClient(t *testing.T) {
	t.Parallel()

	// Arrange
	var (
		gotContainerName string
		gotBucketName    string
	)

	service := flow.NewService(
		stubClient(func(_ context.Context, containerName, bucketName string) (*domain.BucketStats, error) {
			gotContainerName = containerName
			gotBucketName = bucketName

			return domain.NewBucketStats("bucket-id"), nil
		}),
	)

	// Act
	stats, err := service.BucketStats(t.Context(), "rgw", "test")

	// Assert
	require.NoError(t, err)
	require.Equal(t, "rgw", gotContainerName)
	require.Equal(t, "test", gotBucketName)
	require.Equal(t, "bucket-id", stats.ID())
}

func TestServiceBucketStatsReturnsClientError(t *testing.T) {
	t.Parallel()

	// Arrange
	wantErr := errClientFailed
	service := flow.NewService(stubClient(func(context.Context, string, string) (*domain.BucketStats, error) {
		return nil, wantErr
	}))

	// Act
	_, err := service.BucketStats(t.Context(), "rgw", "test")

	// Assert
	require.ErrorIs(t, err, wantErr)
}

type stubClient func(context.Context, string, string) (*domain.BucketStats, error)

func (s stubClient) BucketStats(ctx context.Context, containerName, bucketName string) (*domain.BucketStats, error) {
	return s(ctx, containerName, bucketName)
}

var errClientFailed = errors.New("client failed")

var _ client.Client = stubClient(nil)
