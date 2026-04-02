package domain_test

import (
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestNewVersioningAcceptsAllowedValues(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		value string
		want  domain.Versioning
	}{
		{name: "off", value: "off", want: domain.VersioningOff},
		{name: "suspended", value: "suspended", want: domain.VersioningSuspended},
		{name: "enabled", value: "enabled", want: domain.VersioningEnabled},
	}

	for _, testCase := range testCases {

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, err := domain.NewVersioning(testCase.value)

			require.NoError(t, err)
			require.Equal(t, testCase.want, got)
		})
	}
}

func TestNewVersioningRejectsUnknownValue(t *testing.T) {
	t.Parallel()

	got, err := domain.NewVersioning("mystery")

	require.Error(t, err)
	require.Empty(t, got)
}

func TestNewBucketStatsStoresVersioning(t *testing.T) {
	t.Parallel()

	stats, err := domain.NewBucketStats("bucket-id", 11, "suspended")

	require.NoError(t, err)
	require.Equal(t, "bucket-id", stats.ID())
	require.Equal(t, 11, stats.TotalShards())
	require.Equal(t, domain.VersioningSuspended, stats.Versioning())
}

func TestNewBucketStatsRejectsInvalidVersioning(t *testing.T) {
	t.Parallel()

	stats, err := domain.NewBucketStats("bucket-id", 11, "mystery")

	require.Error(t, err)
	require.Nil(t, stats)
}
