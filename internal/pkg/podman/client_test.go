package podman_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/podman"
	"github.com/stretchr/testify/require"
)

func TestClientBucketStatsRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{"exec", "-i", "rgw", "radosgw-admin", "bucket", "stats", "--bucket=test"}
			require.Equal(t, wantArgs, args)

			return []byte(`{"id":"bucket-id"}`), "", nil
		}),
	)

	// Act
	stats, err := client.BucketStats(t.Context(), "rgw", "test")

	// Assert
	require.NoError(t, err)
	require.Equal(t, "bucket-id", stats.ID())
}

func TestClientBucketStatsParsesFixture(t *testing.T) {
	t.Parallel()

	// Arrange
	fixture, err := os.ReadFile(filepath.Join("testdata", "test.stats.json"))
	require.NoError(t, err)

	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return fixture, "", nil
		}),
	)

	// Act
	stats, err := client.BucketStats(t.Context(), "rgw", "test")

	// Assert
	require.NoError(t, err)
	require.Equal(t, "20135590-8915-4c5e-9328-f759717a4f87.21289.1", stats.ID())
}

func TestClientBucketStatsReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return nil, "permission denied", errExitStatus125
		}),
	)

	// Act
	_, err := client.BucketStats(t.Context(), "rgw", "test")

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), "permission denied")
}

func TestClientBucketStatsReturnsJSONError(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return []byte("{"), "", nil
		}),
	)

	// Act
	_, err := client.BucketStats(t.Context(), "rgw", "test")

	// Assert
	require.Error(t, err)
}

type stubRunner func(context.Context, ...string) ([]byte, string, error)

func (s stubRunner) Run(ctx context.Context, args ...string) ([]byte, string, error) {
	return s(ctx, args...)
}

var (
	_                podman.Runner = stubRunner(nil)
	errExitStatus125               = errors.New("exit status 125")
)
