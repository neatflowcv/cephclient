package podman_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/neatflowcv/cephclient/internal/pkg/podman"
	"github.com/stretchr/testify/require"
)

const errPermissionDenied = "permission denied"

func TestClientBucketStatsRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{"exec", "-i", "rgw", "radosgw-admin", "bucket", "stats", "--bucket=test"}
			require.Equal(t, wantArgs, args)

			return []byte(`{"id":"bucket-id","num_shards":11}`), "", nil
		}),
	)

	// Act
	stats, err := client.BucketStats(t.Context(), "rgw", "test")

	// Assert
	require.NoError(t, err)
	require.Equal(t, "bucket-id", stats.ID())
	require.Equal(t, 11, stats.TotalShards())
}

func TestClientBIListRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{
				"exec",
				"-i",
				"rgw",
				"radosgw-admin",
				"bi",
				"list",
				"--bucket=test-bucket",
				"--object=test-object",
				"--shard-id=7",
			}
			require.Equal(t, wantArgs, args)

			return []byte(`[]`), "", nil
		}),
	)

	// Act
	biList, err := client.BIList(t.Context(), "rgw", "test-bucket", "test-object", 7)

	// Assert
	require.NoError(t, err)
	require.Empty(t, biList.Entries())
}

func TestClientBIListParsesFixture(t *testing.T) {
	t.Parallel()

	// Arrange
	fixture, err := os.ReadFile(filepath.Join("testdata", "test.bilist.json"))
	require.NoError(t, err)

	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return fixture, "", nil
		}),
	)

	// Act
	biList, err := client.BIList(t.Context(), "rgw", "test-bucket", "test.txt", 3)

	// Assert
	require.NoError(t, err)

	entries := biList.Entries()
	require.Len(t, entries, 4)

	plain, okPlain := entries[0].(*domain.PlainBIEntry)
	require.True(t, okPlain)
	require.Equal(t, "test.txt", plain.IDX().Escaped())
	require.Equal(t, "test.txt", plain.Entry().Name())
	require.Empty(t, plain.Entry().Instance())
	require.Equal(t, 0, plain.Entry().VersionedEpoch())
	require.Len(t, plain.Entry().PendingMap(), 1)
	require.Equal(t, "_U-yRh58uJtdkq5PRyYmG5eI7Tpo__5O", plain.Entry().PendingMap()[0].Key())

	plainVersioned, okPlainVersioned := entries[1].(*domain.PlainBIEntry)
	require.True(t, okPlainVersioned)
	require.Equal(t, "test.txt\\x00v913\\x00iPDGqmtJA7imna.RLH.1nsBhSy1ZWf9m", plainVersioned.IDX().Escaped())
	require.Equal(t, "PDGqmtJA7imna.RLH.1nsBhSy1ZWf9m", plainVersioned.Entry().Instance())
	require.Equal(t, 8, plainVersioned.Entry().Ver().Pool())
	require.Equal(t, 119, plainVersioned.Entry().Ver().Epoch())

	instance, okInstance := entries[2].(*domain.InstanceBIEntry)
	require.True(t, okInstance)
	require.Equal(t, "\\x801000_test.txt\\x00iPDGqmtJA7imna.RLH.1nsBhSy1ZWf9m", instance.IDX().Escaped())
	require.Equal(t, "test.txt", instance.Entry().Name())
	require.Equal(t, "PDGqmtJA7imna.RLH.1nsBhSy1ZWf9m", instance.Entry().Instance())

	olh, okOLH := entries[3].(*domain.OLHBIEntry)
	require.True(t, okOLH)
	require.Equal(t, "\\x801001_test.txt", olh.IDX().Escaped())
	require.Equal(t, "test.txt", olh.Entry().Key().Name())
	require.Equal(t, "PDGqmtJA7imna.RLH.1nsBhSy1ZWf9m", olh.Entry().Key().Instance())
	require.Equal(t, 2, olh.Entry().Epoch())
	require.Len(t, olh.Entry().PendingLog(), 1)
	require.Len(t, olh.Entry().PendingLog()[0].Val(), 2)
}

func TestClientBIListReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		}),
	)

	// Act
	_, err := client.BIList(t.Context(), "rgw", "test-bucket", "test-object", 7)

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
}

func TestClientBIListReturnsJSONError(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return []byte("{"), "", nil
		}),
	)

	// Act
	_, err := client.BIList(t.Context(), "rgw", "test-bucket", "test-object", 7)

	// Assert
	require.Error(t, err)
}

func TestClientBIListRejectsUnknownType(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return []byte(`[{"type":"mystery","idx":"x","entry":{}}]`), "", nil
		}),
	)

	// Act
	_, err := client.BIList(t.Context(), "rgw", "test-bucket", "test-object", 7)

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported bi entry type")
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
	require.Equal(t, 11, stats.TotalShards())
}

func TestClientBucketStatsReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		}),
	)

	// Act
	_, err := client.BucketStats(t.Context(), "rgw", "test")

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
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

func TestClientListBucketsRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{"exec", "-i", "rgw", "radosgw-admin", "bucket", "list"}
			require.Equal(t, wantArgs, args)

			return []byte(`["alpha","beta"]`), "", nil
		}),
	)

	// Act
	buckets, err := client.ListBuckets(t.Context(), "rgw")

	// Assert
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "beta"}, buckets)
}

func TestClientListBucketsReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		}),
	)

	// Act
	_, err := client.ListBuckets(t.Context(), "rgw")

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
}

func TestClientListBucketsReturnsJSONError(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return []byte("{"), "", nil
		}),
	)

	// Act
	_, err := client.ListBuckets(t.Context(), "rgw")

	// Assert
	require.Error(t, err)
}

func TestClientObjectShardRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{
				"exec",
				"-i",
				"rgw",
				"radosgw-admin",
				"bucket",
				"object",
				"shard",
				"--object=test-object",
				"--num-shards=11",
			}
			require.Equal(t, wantArgs, args)

			return []byte(`{"shard":0}`), "", nil
		}),
	)

	// Act
	shard, err := client.ObjectShard(t.Context(), "rgw", "test-object", 11)

	// Assert
	require.NoError(t, err)
	require.Equal(t, 0, shard.Shard())
}

func TestClientObjectShardParsesFixture(t *testing.T) {
	t.Parallel()

	// Arrange
	fixture, err := os.ReadFile(filepath.Join("testdata", "test.shard.json"))
	require.NoError(t, err)

	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return fixture, "", nil
		}),
	)

	// Act
	shard, err := client.ObjectShard(t.Context(), "rgw", "test-object", 11)

	// Assert
	require.NoError(t, err)
	require.Equal(t, 0, shard.Shard())
}

func TestClientObjectShardReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		}),
	)

	// Act
	_, err := client.ObjectShard(t.Context(), "rgw", "test-object", 11)

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
}

func TestClientObjectShardReturnsJSONError(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return []byte("{"), "", nil
		}),
	)

	// Act
	_, err := client.ObjectShard(t.Context(), "rgw", "test-object", 11)

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
