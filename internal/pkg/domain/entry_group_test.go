package domain_test

import (
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestEntryGroupIsEmpty(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		group := domain.NewEntryGroup(nil, nil, nil)

		require.True(t, group.IsEmpty())
	})

	t.Run("not empty", func(t *testing.T) {
		t.Parallel()

		group := domain.NewEntryGroup(
			nil,
			nil,
			[]*domain.InstanceBIEntry{
				domain.NewInstanceBIEntry(
					domain.NewBIIndex("test.txt-instance:instance-1"),
					domain.NewBIObjectEntry(
						"test.txt",
						"instance-1",
						domain.NewBIVersion(8, 119),
						"",
						true,
						domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
						"",
						0,
						false,
						2,
					),
				),
			},
		)

		require.False(t, group.IsEmpty())
	})
}

func TestEntryGroupExtractRawObjectNames(t *testing.T) {
	t.Parallel()

	group := newEntryGroupForExtractionTests()

	names := group.ExtractRawObjectNames("bucket-marker", "test.txt")

	require.Equal(
		t,
		[]string{
			"bucket-marker_test.txt",
			"bucket-marker__:instance-1_test.txt",
			"bucket-marker__:instance-2_test.txt",
		},
		names,
	)
}

func TestEntryGroupExtractOmapKeys(t *testing.T) {
	t.Parallel()

	group := newEntryGroupForExtractionTests()

	keys := group.ExtractOmapKeys()

	require.Equal(
		t,
		[]string{"test.txt", "test.txt:instance-2", "test.txt:instance-1", "test.txt-instance:instance-1"},
		keys,
	)
}

//nolint:funlen // Shared extraction fixture keeps raw and omap expectations aligned.
func newEntryGroupForExtractionTests() *domain.EntryGroup {
	return domain.NewEntryGroup(
		[]*domain.OLHBIEntry{
			domain.NewOLHBIEntry(domain.OLHBIEntryParams{
				IDX: domain.NewBIIndex("test.txt"),
				Entry: domain.NewBIOLHEntry(
					domain.NewBIOLHKey("test.txt", "instance-1"),
					false,
					2,
					nil,
					"",
					true,
					false,
				),
			}),
		},
		[]*domain.PlainBIEntry{
			domain.NewPlainBIEntry(
				domain.NewBIIndex("test.txt:instance-2"),
				domain.NewBIObjectEntry(
					"test.txt",
					"instance-2",
					domain.NewBIVersion(8, 120),
					"",
					true,
					domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
					"",
					0,
					false,
					2,
				),
			),
			domain.NewPlainBIEntry(
				domain.NewBIIndex("test.txt:instance-1"),
				domain.NewBIObjectEntry(
					"test.txt",
					"instance-1",
					domain.NewBIVersion(8, 119),
					"",
					true,
					domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
					"",
					0,
					false,
					2,
				),
			),
		},
		[]*domain.InstanceBIEntry{
			domain.NewInstanceBIEntry(
				domain.NewBIIndex("test.txt-instance:instance-1"),
				domain.NewBIObjectEntry(
					"test.txt",
					"instance-1",
					domain.NewBIVersion(8, 119),
					"",
					true,
					domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
					"",
					0,
					false,
					2,
				),
			),
			domain.NewInstanceBIEntry(
				domain.NewBIIndex("test.txt-instance:instance-1"),
				domain.NewBIObjectEntry(
					"test.txt",
					"instance-1",
					domain.NewBIVersion(8, 119),
					"",
					true,
					domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
					"",
					0,
					false,
					2,
				),
			),
		},
	)
}
