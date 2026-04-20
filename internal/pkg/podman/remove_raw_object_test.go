package podman_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/podman"
	"github.com/stretchr/testify/require"
)

func TestClientRemoveRawObjectRunsPodmanCommand(t *testing.T) {
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
				"rm",
				"marker__:test-object",
			}
			require.Equal(t, wantArgs, args)

			return nil, "", nil
		},
	)
	client := podman.NewClientWithRunner(runner)

	err := client.RemoveRawObject(t.Context(), "rgw", "default.rgw.buckets.data", "marker__:test-object")

	require.NoError(t, err)
	require.Len(t, runner.RunCalls(), 1)
}

func TestClientRemoveRawObjectReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		},
	))

	err := client.RemoveRawObject(t.Context(), "rgw", "default.rgw.buckets.data", "marker__:test-object")

	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
	require.Contains(t, err.Error(), "rados -p default.rgw.buckets.data rm marker__:test-object")
}

func TestClientRemoveRawObjectReturnsNotFoundError(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil,
				"error removing default.rgw.buckets.data/marker__:test-object: (2) No such file or directory",
				errExitStatus1
		},
	))

	err := client.RemoveRawObject(t.Context(), "rgw", "default.rgw.buckets.data", "marker__:test-object")

	require.Error(t, err)
	require.Contains(t, err.Error(), "No such file or directory")
	require.Contains(t, err.Error(), "rados -p default.rgw.buckets.data rm marker__:test-object")
}
