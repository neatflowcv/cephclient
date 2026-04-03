package podman_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/podman"
	"github.com/stretchr/testify/require"
)

func TestClientRemoveObjectRunsPodmanCommand(t *testing.T) {
	t.Parallel()

	runner := newRunnerMock(
		func(_ context.Context, args ...string) ([]byte, string, error) {
			wantArgs := []string{
				"exec",
				"-i",
				"rgw",
				"radosgw-admin",
				"object",
				"rm",
				"--bucket=test-bucket",
				"--object=test-object",
				"--object-version=version-1",
			}
			require.Equal(t, wantArgs, args)

			return nil, "", nil
		},
	)
	client := podman.NewClientWithRunner(runner)

	err := client.RemoveObject(t.Context(), "rgw", "test-bucket", "test-object", "version-1")

	require.NoError(t, err)
	require.Len(t, runner.RunCalls(), 1)
}

func TestClientRemoveObjectReturnsRunnerErrorWithStderr(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		},
	))

	err := client.RemoveObject(t.Context(), "rgw", "test-bucket", "test-object", "version-1")

	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
	require.Contains(t, err.Error(), "radosgw-admin object rm")
}
