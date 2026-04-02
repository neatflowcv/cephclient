package domain_test

import (
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestNewBucketStatsAcceptsAllowedVersioningStatuses(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		value string
		want  domain.VersioningStatus
	}{
		{name: "off", value: "off", want: domain.VersioningStatusOff},
		{name: "suspended", value: "suspended", want: domain.VersioningStatusSuspended},
		{name: "enabled", value: "enabled", want: domain.VersioningStatusEnabled},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			stats, err := domain.NewBucketStats("bucket-id", 11, "bucket-marker", testCase.want)

			require.NoError(t, err)
			require.Equal(t, "bucket-marker", stats.Marker())
			require.Equal(t, testCase.want, stats.Versioning())
		})
	}
}

func TestNewBucketStatsRejectsUnknownVersioningStatus(t *testing.T) {
	t.Parallel()

	stats, err := domain.NewBucketStats("bucket-id", 11, "bucket-marker", domain.VersioningStatus("mystery"))

	require.Error(t, err)
	require.Nil(t, stats)
}

func TestNewBucketStatsStoresVersioning(t *testing.T) {
	t.Parallel()

	stats, err := domain.NewBucketStats("bucket-id", 11, "bucket-marker", domain.VersioningStatusSuspended)

	require.NoError(t, err)
	require.Equal(t, "bucket-id", stats.ID())
	require.Equal(t, 11, stats.TotalShards())
	require.Equal(t, "bucket-marker", stats.Marker())
	require.Equal(t, domain.VersioningStatusSuspended, stats.Versioning())
}

func TestNewBucketStatsRejectsInvalidVersioning(t *testing.T) {
	t.Parallel()

	stats, err := domain.NewBucketStats("bucket-id", 11, "bucket-marker", domain.VersioningStatus("mystery"))

	require.Error(t, err)
	require.Nil(t, stats)
}
