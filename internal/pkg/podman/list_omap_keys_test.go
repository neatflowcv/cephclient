package podman_test

import (
	"context"
	"errors"
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/neatflowcv/cephclient/internal/pkg/podman"
	"github.com/stretchr/testify/require"
)

const listOmapKeysErrPermissionDenied = "permission denied"

func TestClientListOmapKeysRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	runner := newRunnerMock(
		func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{
				"exec",
				"-i",
				"rgw",
				"rados",
				"-p",
				"default.rgw.buckets.index",
				"listomapkeys",
				".dir.bucket-marker.7",
			}
			require.Equal(t, wantArgs, args)

			return []byte("plain\n" + string([]byte{0x80}) + "quoted\"value\n"), "", nil
		},
	)
	client := podman.NewClientWithRunner(runner)
	indexObject := domain.NewBucketIndexObject("bucket-marker", 7)

	indexes, err := client.ListOmapKeys(t.Context(), "rgw", "default.rgw.buckets.index", indexObject)

	require.NoError(t, err)
	require.Len(t, runner.RunCalls(), 1)
	require.Len(t, indexes, 2)
	require.Equal(t, "plain", indexes[0].Escaped())
	require.Equal(t, "\\x80quoted\"value", indexes[1].Escaped())
}

func TestClientListOmapKeysReturnsEmptySliceForEmptyOutput(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, "", nil
		},
	))
	indexObject := domain.NewBucketIndexObject("bucket-marker", 7)

	indexes, err := client.ListOmapKeys(t.Context(), "rgw", "default.rgw.buckets.index", indexObject)

	require.NoError(t, err)
	require.Empty(t, indexes)
}

func TestClientListOmapKeysReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, listOmapKeysErrPermissionDenied, errListOmapKeysExitStatus125
		},
	))
	indexObject := domain.NewBucketIndexObject("bucket-marker", 7)

	_, err := client.ListOmapKeys(t.Context(), "rgw", "default.rgw.buckets.index", indexObject)

	require.Error(t, err)
	require.Contains(t, err.Error(), listOmapKeysErrPermissionDenied)
}

var errListOmapKeysExitStatus125 = errors.New("exit status 125")
