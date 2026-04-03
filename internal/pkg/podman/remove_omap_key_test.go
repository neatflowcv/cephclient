package podman_test

import (
	"context"
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/podman"
	"github.com/stretchr/testify/require"
)

func TestClientRemoveOmapKeyRunsPodmanCommandsInOrder(t *testing.T) {
	t.Parallel()

	// Arrange
	callCount := 0
	client := podman.NewClientWithRunner(
		stubRunner(func(_ context.Context, args ...string) ([]byte, string, error) {
			callCount++

			switch callCount {
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
				t.Fatalf("unexpected extra runner call %d", callCount)

				return nil, "", nil
			}
		}),
	)

	// Act
	err := client.RemoveOmapKey(
		t.Context(),
		"rgw",
		"default.rgw.buckets.index",
		"bucket-marker",
		7,
		"plain-key",
	)

	// Assert
	require.NoError(t, err)
	require.Equal(t, 3, callCount)
}

func TestClientRemoveOmapKeyReturnsMktempErrorWithStderr(t *testing.T) {
	t.Parallel()

	// Arrange
	client := podman.NewClientWithRunner(
		stubRunner(func(context.Context, ...string) ([]byte, string, error) {
			return nil, errPermissionDenied, errExitStatus125
		}),
	)

	// Act
	err := client.RemoveOmapKey(
		t.Context(),
		"rgw",
		"default.rgw.buckets.index",
		"bucket-marker",
		7,
		"plain-key",
	)

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
	require.Contains(t, err.Error(), "mktemp")
}

func TestClientRemoveOmapKeyReturnsWriteKeyErrorWithStderr(t *testing.T) {
	t.Parallel()

	// Arrange
	callCount := 0
	client := podman.NewClientWithRunner(
		stubRunner(func(_ context.Context, args ...string) ([]byte, string, error) {
			callCount++

			switch callCount {
			case 1:
				require.Equal(t, []string{"exec", "-i", "rgw", "mktemp"}, args)

				return []byte("/tmp/remove-omap-key\n"), "", nil
			case 2:
				return nil, errPermissionDenied, errExitStatus125
			default:
				t.Fatalf("unexpected runner call %d", callCount)

				return nil, "", nil
			}
		}),
	)

	// Act
	err := client.RemoveOmapKey(
		t.Context(),
		"rgw",
		"default.rgw.buckets.index",
		"bucket-marker",
		7,
		"plain-key",
	)

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
	require.Contains(t, err.Error(), `printf "plain-key" > "/tmp/remove-omap-key"`)
}

func TestClientRemoveOmapKeyReturnsRemoveErrorWithStderr(t *testing.T) {
	t.Parallel()

	// Arrange
	callCount := 0
	client := podman.NewClientWithRunner(
		stubRunner(func(_ context.Context, args ...string) ([]byte, string, error) {
			callCount++

			switch callCount {
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
				t.Fatalf("unexpected runner call %d", callCount)

				return nil, "", nil
			}
		}),
	)

	// Act
	err := client.RemoveOmapKey(
		t.Context(),
		"rgw",
		"default.rgw.buckets.index",
		"bucket-marker",
		7,
		"plain-key",
	)

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), errPermissionDenied)
	require.Contains(t, err.Error(), "rmomapkey")
}
