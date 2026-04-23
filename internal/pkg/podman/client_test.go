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

func TestClientBucketStatsMapsBucketAndUsageFieldsFromJSON(t *testing.T) {
	t.Parallel()

	runner := newRunnerMock(
		func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{"exec", "-i", "rgw", "radosgw-admin", "bucket", "stats", "--bucket=test"}
			require.Equal(t, wantArgs, args)

			return []byte(`{
				"id":"bucket-id",
				"bucket":"test",
				"num_shards":11,
				"marker":"bucket-marker",
				"usage":{"rgw.main":{"size":5,"num_objects":1}},
				"versioning":"enabled"
			}`), "", nil
		},
	)
	client := podman.NewClientWithRunner(runner)

	stats, err := client.BucketStats(t.Context(), "rgw", "test")

	require.NoError(t, err)
	require.Len(t, runner.RunCalls(), 1)
	require.Equal(t, "test", stats.Name())   // json: bucket
	require.EqualValues(t, 5, stats.Size())  // json: usage.rgw.main.size
	require.Equal(t, 1, stats.ObjectCount()) // json: usage.rgw.main.num_objects
	require.Equal(t, "bucket-id", stats.ID())
	require.Equal(t, 11, stats.TotalShards())
	require.Equal(t, "bucket-marker", stats.Marker())
	require.Equal(t, domain.VersioningStatusEnabled, stats.Versioning())
}

func TestClientBucketLayoutRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	runner := newRunnerMock(
		func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{"exec", "-i", "rgw", "radosgw-admin", "bucket", "layout", "--bucket=test"}
			require.Equal(t, wantArgs, args)

			return []byte(`{"layout":{"current_index":{"gen":1}}}`), "", nil
		},
	)
	client := podman.NewClientWithRunner(runner)

	layout, err := client.BucketLayout(t.Context(), "rgw", "test")

	require.NoError(t, err)
	require.Len(t, runner.RunCalls(), 1)
	require.Equal(t, 1, layout.Generation())
}

func TestClientBIListByShardRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	runner := newRunnerMock(
		func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{
				"exec",
				"-i",
				"rgw",
				"radosgw-admin",
				"bi",
				"list",
				"--bucket=test-bucket",
				"--shard-id=7",
			}
			require.Equal(t, wantArgs, args)

			return []byte(`[]`), "", nil
		},
	)
	client := podman.NewClientWithRunner(runner)

	biList, err := client.BIListByShard(t.Context(), "rgw", "test-bucket", 7)

	require.NoError(t, err)
	require.Len(t, runner.RunCalls(), 1)
	require.Empty(t, biList.Entries())
}

func TestClientBIListByObjectRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	runner := newRunnerMock(
		func(_ context.Context, args ...string) ([]byte, string, error) {
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
		},
	)
	client := podman.NewClientWithRunner(runner)

	biList, err := client.ListBIByObject(t.Context(), "rgw", "test-bucket", "test-object", 7)

	require.NoError(t, err)
	require.Len(t, runner.RunCalls(), 1)
	require.Empty(t, biList.Entries())
}

func TestClientBIListByObjectParsesFixture(t *testing.T) {
	t.Parallel()

	fixture, err := os.ReadFile(filepath.Join("testdata", "test.bilist.json"))
	require.NoError(t, err)

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return fixture, "", nil
		},
	))

	biList, err := client.ListBIByObject(t.Context(), "rgw", "test-bucket", "test.txt", 3)

	require.NoError(t, err)

	entries := biList.Entries()
	require.Len(t, entries, 4)

	plain, okPlain := entries[0].(*domain.PlainBIEntry)
	require.True(t, okPlain)
	require.Equal(t, "test.txt", plain.IDX().Escaped())
	require.Equal(t, "test.txt", plain.Entry().Name())
	require.Empty(t, plain.Entry().Instance())
	require.Equal(t, 0, plain.Entry().VersionedEpoch())
	require.True(t, plain.Entry().Pending())

	plainVersioned, okPlainVersioned := entries[1].(*domain.PlainBIEntry)
	require.True(t, okPlainVersioned)
	require.Equal(t, "test.txt\\x00v913\\x00iPDGqmtJA7imna.RLH.1nsBhSy1ZWf9m", plainVersioned.IDX().Escaped())
	require.Equal(t, "PDGqmtJA7imna.RLH.1nsBhSy1ZWf9m", plainVersioned.Entry().Instance())
	require.Equal(t, 8, plainVersioned.Entry().Ver().Pool())
	require.Equal(t, 119, plainVersioned.Entry().Ver().Epoch())
	require.False(t, plainVersioned.Entry().Pending())

	instance, okInstance := entries[2].(*domain.InstanceBIEntry)
	require.True(t, okInstance)
	require.Equal(t, "\\x801000_test.txt\\x00iPDGqmtJA7imna.RLH.1nsBhSy1ZWf9m", instance.IDX().Escaped())
	require.Equal(t, "test.txt", instance.Entry().Name())
	require.Equal(t, "PDGqmtJA7imna.RLH.1nsBhSy1ZWf9m", instance.Entry().Instance())

	olh, okOLH := entries[3].(*domain.OLH)
	require.True(t, okOLH)
	require.Equal(t, "\\x801001_test.txt", olh.IDX().Escaped())
	require.Equal(t, "test.txt", olh.Name())
	require.Equal(t, "PDGqmtJA7imna.RLH.1nsBhSy1ZWf9m", olh.Instance())
	require.Equal(t, 2, olh.Epoch())
	require.Len(t, olh.PendingLog(), 1)
	require.Len(t, olh.ReferencedVersions(), 3)
}

func TestClientBIListByObjectReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		},
	))

	_, err := client.ListBIByObject(t.Context(), "rgw", "test-bucket", "test-object", 7)

	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
}

func TestClientBIListByObjectReturnsJSONError(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return []byte("{"), "", nil
		},
	))

	_, err := client.ListBIByObject(t.Context(), "rgw", "test-bucket", "test-object", 7)

	require.Error(t, err)
}

func TestClientBIListByObjectRejectsUnknownType(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return []byte(`[{"type":"mystery","idx":"x","entry":{}}]`), "", nil
		},
	))

	_, err := client.ListBIByObject(t.Context(), "rgw", "test-bucket", "test-object", 7)

	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported bi entry type")
}

func TestClientBucketStatsParsesFixtureBucketAndUsagePaths(t *testing.T) {
	t.Parallel()

	fixture, err := os.ReadFile(filepath.Join("testdata", "test.stats.json"))
	require.NoError(t, err)

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return fixture, "", nil
		},
	))

	stats, err := client.BucketStats(t.Context(), "rgw", "test")

	require.NoError(t, err)
	require.Equal(t, "test", stats.Name())   // json: bucket
	require.EqualValues(t, 5, stats.Size())  // json: usage.rgw.main.size
	require.Equal(t, 1, stats.ObjectCount()) // json: usage.rgw.main.num_objects
	require.Equal(t, "20135590-8915-4c5e-9328-f759717a4f87.21289.1", stats.ID())
	require.Equal(t, 11, stats.TotalShards())
	require.Equal(t, "20135590-8915-4c5e-9328-f759717a4f87.21289.1", stats.Marker())
	require.Equal(t, domain.VersioningStatusEnabled, stats.Versioning())
}

func TestClientBucketStatsReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		},
	))

	_, err := client.BucketStats(t.Context(), "rgw", "test")

	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
}

func TestClientBucketStatsReturnsJSONError(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return []byte("{"), "", nil
		},
	))

	_, err := client.BucketStats(t.Context(), "rgw", "test")

	require.Error(t, err)
}

func TestClientBucketStatsReturnsInvalidVersioningError(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return []byte(`{
				"id":"bucket-id",
				"bucket":"test",
				"num_shards":11,
				"marker":"bucket-marker",
				"usage":{"rgw.main":{"size":5,"num_objects":1}},
				"versioning":"mystery"
			}`), "", nil
		},
	))

	_, err := client.BucketStats(t.Context(), "rgw", "test")

	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid versioning")
}

func TestClientBucketLayoutParsesFixture(t *testing.T) {
	t.Parallel()

	fixture, err := os.ReadFile(filepath.Join("testdata", "test.layout.json"))
	require.NoError(t, err)

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return fixture, "", nil
		},
	))

	layout, err := client.BucketLayout(t.Context(), "rgw", "test")

	require.NoError(t, err)
	require.Equal(t, 1, layout.Generation())
}

func TestClientBucketLayoutReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		},
	))

	_, err := client.BucketLayout(t.Context(), "rgw", "test")

	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
}

func TestClientBucketLayoutReturnsJSONError(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return []byte("{"), "", nil
		},
	))

	_, err := client.BucketLayout(t.Context(), "rgw", "test")

	require.Error(t, err)
}

func TestClientGetDefaultZoneRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	runner := newRunnerMock(
		func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{"exec", "-i", "rgw", "radosgw-admin", "zone", "get"}
			require.Equal(t, wantArgs, args)

			return []byte(
				`{
					"placement_pools": [{
						"val": {
							"index_pool": "test.rgw.buckets.index",
							"storage_classes": {"STANDARD": {"data_pool": "test.rgw.buckets.data"}}
						}
					}]
				}`,
			), "", nil
		},
	)
	client := podman.NewClientWithRunner(runner)

	zone, err := client.GetDefaultZone(t.Context(), "rgw")

	require.NoError(t, err)
	require.Len(t, runner.RunCalls(), 1)
	require.Equal(t, "test.rgw.buckets.data", zone.DataPool())
	require.Equal(t, "test.rgw.buckets.index", zone.IndexPool())
}

func TestClientGetDefaultZoneParsesFixture(t *testing.T) {
	t.Parallel()

	fixture, err := os.ReadFile(filepath.Join("testdata", "test.zone.json"))
	require.NoError(t, err)

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return fixture, "", nil
		},
	))

	zone, err := client.GetDefaultZone(t.Context(), "rgw")

	require.NoError(t, err)
	require.Equal(t, "test.rgw.buckets.data", zone.DataPool())
	require.Equal(t, "test.rgw.buckets.index", zone.IndexPool())
}

func TestClientGetDefaultZoneReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		},
	))

	_, err := client.GetDefaultZone(t.Context(), "rgw")

	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
}

func TestClientGetDefaultZoneReturnsErrorWhenPlacementPoolsAreEmpty(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return []byte(`{"placement_pools":[]}`), "", nil
		},
	))

	_, err := client.GetDefaultZone(t.Context(), "rgw")

	require.Error(t, err)
	require.Contains(t, err.Error(), "placement_pools is empty")
}

func TestClientGetDefaultZoneReturnsErrorWhenStandardStorageClassIsMissing(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return []byte(`{"placement_pools":[{"val":{"index_pool":"test.rgw.buckets.index","storage_classes":{}}}]}`), "", nil
		},
	))

	_, err := client.GetDefaultZone(t.Context(), "rgw")

	require.Error(t, err)
	require.Contains(t, err.Error(), "STANDARD storage class not found")
}

func TestClientGetDefaultZoneReturnsErrorWhenRequiredPoolValuesAreMissing(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return []byte(
				`{"placement_pools":[{"val":{"index_pool":"","storage_classes":{"STANDARD":{"data_pool":""}}}}]}`,
			), "", nil
		},
	))

	_, err := client.GetDefaultZone(t.Context(), "rgw")

	require.Error(t, err)
	require.Contains(t, err.Error(), "index_pool is empty")
}

func TestClientListBucketsRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	runner := newRunnerMock(
		func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{"exec", "-i", "rgw", "radosgw-admin", "bucket", "list"}
			require.Equal(t, wantArgs, args)

			return []byte(`["alpha","beta"]`), "", nil
		},
	)
	client := podman.NewClientWithRunner(runner)

	buckets, err := client.ListBuckets(t.Context(), "rgw")

	require.NoError(t, err)
	require.Len(t, runner.RunCalls(), 1)
	require.Equal(t, []string{"alpha", "beta"}, buckets)
}

func TestClientListBucketsReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		},
	))

	_, err := client.ListBuckets(t.Context(), "rgw")

	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
}

func TestClientListBucketsReturnsJSONError(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return []byte("{"), "", nil
		},
	))

	_, err := client.ListBuckets(t.Context(), "rgw")

	require.Error(t, err)
}

func TestClientObjectShardRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	runner := newRunnerMock(
		func(_ context.Context, args ...string) ([]byte, string, error) {
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
		},
	)
	client := podman.NewClientWithRunner(runner)

	shard, err := client.ObjectShard(t.Context(), "rgw", "test-object", 11)

	require.NoError(t, err)
	require.Len(t, runner.RunCalls(), 1)
	require.Equal(t, 0, shard.Shard())
}

func TestClientObjectShardParsesFixture(t *testing.T) {
	t.Parallel()

	fixture, err := os.ReadFile(filepath.Join("testdata", "test.shard.json"))
	require.NoError(t, err)

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return fixture, "", nil
		},
	))

	shard, err := client.ObjectShard(t.Context(), "rgw", "test-object", 11)

	require.NoError(t, err)
	require.Equal(t, 0, shard.Shard())
}

func TestClientObjectShardReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		},
	))

	_, err := client.ObjectShard(t.Context(), "rgw", "test-object", 11)

	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
}

func TestClientObjectShardReturnsJSONError(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return []byte("{"), "", nil
		},
	))

	_, err := client.ObjectShard(t.Context(), "rgw", "test-object", 11)

	require.Error(t, err)
}

var errExitStatus125 = errors.New("exit status 125")

//nolint:exhaustruct
func newRunnerMock(runFunc func(context.Context, ...string) ([]byte, string, error)) *RunnerMock {
	return &RunnerMock{RunFunc: runFunc}
}

//nolint:exhaustruct
func newUnsetRunnerMock() *RunnerMock {
	return &RunnerMock{}
}
