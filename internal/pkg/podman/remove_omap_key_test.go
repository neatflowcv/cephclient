package podman_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/podman"
	"github.com/stretchr/testify/require"
)

func TestClientRemoveOmapKeyRunsPodmanCommandsInOrder(t *testing.T) {
	t.Parallel()

	runner := newUnsetRunnerMock()
	runner.RunFunc = func(_ context.Context, args ...string) ([]byte, string, error) {
		switch len(runner.RunCalls()) {
		case 1:
			require.Equal(t, []string{"exec", "-i", "rgw", "mktemp"}, args)

			return []byte("/tmp/remove-omap-key\n"), "", nil
		case 2:
			require.Equal(t, []string{
				"exec",
				"-i",
				"rgw",
				"sh",
				"-c",
				`printf "plain-key" > "/tmp/remove-omap-key"`,
			}, args)

			return nil, "", nil
		case 3:
			require.Equal(t, []string{
				"exec",
				"-i",
				"rgw",
				"rados",
				"-p",
				"default.rgw.buckets.index",
				"rmomapkey",
				".dir.bucket-marker.7",
				"--omap-key-file=/tmp/remove-omap-key",
			}, args)

			return nil, "", nil
		default:
			t.Fatalf("unexpected extra runner call %d", len(runner.RunCalls()))

			return nil, "", nil
		}
	}
	client := podman.NewClientWithRunner(runner)

	err := client.RemoveOmapKey(
		t.Context(),
		"rgw",
		"default.rgw.buckets.index",
		"bucket-marker",
		7,
		"plain-key",
	)

	require.NoError(t, err)
	require.Len(t, runner.RunCalls(), 3)
}

func TestClientRemoveOmapKeyReturnsMktempErrorWithStderr(t *testing.T) {
	t.Parallel()

	client := podman.NewClientWithRunner(newRunnerMock(
		func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		},
	))

	err := client.RemoveOmapKey(
		t.Context(),
		"rgw",
		"default.rgw.buckets.index",
		"bucket-marker",
		7,
		"plain-key",
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
	require.Contains(t, err.Error(), "mktemp")
}

func TestClientRemoveOmapKeyReturnsWriteKeyErrorWithStderr(t *testing.T) {
	t.Parallel()

	runner := newUnsetRunnerMock()
	runner.RunFunc = func(_ context.Context, args ...string) ([]byte, string, error) {
		switch len(runner.RunCalls()) {
		case 1:
			require.Equal(t, []string{"exec", "-i", "rgw", "mktemp"}, args)

			return []byte("/tmp/remove-omap-key\n"), "", nil
		case 2:
			return nil, errPermissionDenied, errExitStatus125
		default:
			t.Fatalf("unexpected runner call %d", len(runner.RunCalls()))

			return nil, "", nil
		}
	}
	client := podman.NewClientWithRunner(runner)

	err := client.RemoveOmapKey(
		t.Context(),
		"rgw",
		"default.rgw.buckets.index",
		"bucket-marker",
		7,
		"plain-key",
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
	require.Contains(t, err.Error(), `printf "plain-key" > "/tmp/remove-omap-key"`)
}

func TestClientRemoveOmapKeyReturnsRemoveErrorWithStderr(t *testing.T) {
	t.Parallel()

	runner := newUnsetRunnerMock()
	runner.RunFunc = func(_ context.Context, args ...string) ([]byte, string, error) {
		switch len(runner.RunCalls()) {
		case 1:
			return []byte("/tmp/remove-omap-key\n"), "", nil
		case 2:
			return nil, "", nil
		case 3:
			require.Equal(t, []string{
				"exec",
				"-i",
				"rgw",
				"rados",
				"-p",
				"default.rgw.buckets.index",
				"rmomapkey",
				".dir.bucket-marker.7",
				"--omap-key-file=/tmp/remove-omap-key",
			}, args)

			return nil, errPermissionDenied, errExitStatus125
		default:
			t.Fatalf("unexpected runner call %d", len(runner.RunCalls()))

			return nil, "", nil
		}
	}
	client := podman.NewClientWithRunner(runner)

	err := client.RemoveOmapKey(
		t.Context(),
		"rgw",
		"default.rgw.buckets.index",
		"bucket-marker",
		7,
		"plain-key",
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
	require.Contains(t, err.Error(), "rmomapkey")
}
