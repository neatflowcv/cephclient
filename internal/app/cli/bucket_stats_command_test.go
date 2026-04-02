//nolint:testpackage // writeBucketStats is package-private and this test verifies CLI output formatting directly.
package cli

import (
	"bytes"
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestWriteBucketStatsIncludesVersioning(t *testing.T) {
	t.Parallel()

	stats, err := domain.NewBucketStats("bucket-id", 11, domain.VersioningStatusEnabled)
	require.NoError(t, err)

	var stdout bytes.Buffer

	err = writeBucketStats(&stdout, stats)

	require.NoError(t, err)
	require.Equal(t, "id=bucket-id\ntotal_shards=11\nversioning=enabled\n", stdout.String())
}
