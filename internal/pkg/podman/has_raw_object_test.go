package podman_test

import (
	"context"
	"errors"
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/podman"
	"github.com/stretchr/testify/require"
)

func TestClientHasRawObjectRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	runner := newRunnerMock(
		func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{
				"exec",
				"-i",
				"rgw",
				"rados",
				"-p",
				"default.rgw.buckets.data",
				"stat",
				"marker__:version_object",
			}
			require.Equal(t, wantArgs, args)

			return []byte("size 1 mtime 2026-04-06"), "", nil
		},
	)
	client := podman.NewClientWithRunner(runner)

	exists, err := client.HasRawObject(t.Context(), "rgw", "default.rgw.buckets.data", "marker__:version_object")

	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, runner.RunCalls(), 1)
}

func TestClientHasRawObjectReturnsFalseWhenObjectDoesNotExist(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, "error stat-ing default.rgw.buckets.data/raw-object: (2) No such file or directory", errExitStatus1
		},
	))

	exists, err := client.HasRawObject(t.Context(), "rgw", "default.rgw.buckets.data", "raw-object")

	require.NoError(t, err)
	require.False(t, exists)
}

func TestClientHasRawObjectReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		},
	))

	exists, err := client.HasRawObject(t.Context(), "rgw", "default.rgw.buckets.data", "raw-object")

	require.Error(t, err)
	require.False(t, exists)
	require.Contains(t, err.Error(), errPermissionDenied)
	require.Contains(t, err.Error(), "rados -p default.rgw.buckets.data stat raw-object")
}

var errExitStatus1 = errors.New("exit status 1")
