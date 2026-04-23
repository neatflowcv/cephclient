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
			[]*domain.Instance{
				domain.NewInstance(domain.DirParams{
					Name:           "test.txt",
					Instance:       "instance-1",
					Ver:            domain.NewBIVersion(8, 119),
					Locator:        "",
					Exists:         true,
					Meta:           domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
					Tag:            "",
					Flags:          0,
					Pending:        false,
					VersionedEpoch: 2,
					IDX:            domain.NewBIIndex("test.txt-instance:instance-1"),
				}),
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
		[]*domain.OLH{
			domain.NewOLH(domain.OLHParams{
				DeleteMarker:   false,
				Epoch:          2,
				Exists:         true,
				Instance:       "instance-1",
				Name:           "test.txt",
				PendingLog:     nil,
				PendingRemoval: false,
				Tag:            "",
				IDX:            domain.NewBIIndex("test.txt"),
			}),
		},
		[]*domain.Plain{
			domain.NewPlain(domain.DirParams{
				Name:           "test.txt",
				Instance:       "instance-2",
				Ver:            domain.NewBIVersion(8, 120),
				Locator:        "",
				Exists:         true,
				Meta:           domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
				Tag:            "",
				Flags:          0,
				Pending:        false,
				VersionedEpoch: 2,
				IDX:            domain.NewBIIndex("test.txt:instance-2"),
			}),
			domain.NewPlain(domain.DirParams{
				Name:           "test.txt",
				Instance:       "instance-1",
				Ver:            domain.NewBIVersion(8, 119),
				Locator:        "",
				Exists:         true,
				Meta:           domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
				Tag:            "",
				Flags:          0,
				Pending:        false,
				VersionedEpoch: 2,
				IDX:            domain.NewBIIndex("test.txt:instance-1"),
			}),
		},
		[]*domain.Instance{
			domain.NewInstance(domain.DirParams{
				Name:           "test.txt",
				Instance:       "instance-1",
				Ver:            domain.NewBIVersion(8, 119),
				Locator:        "",
				Exists:         true,
				Meta:           domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
				Tag:            "",
				Flags:          0,
				Pending:        false,
				VersionedEpoch: 2,
				IDX:            domain.NewBIIndex("test.txt-instance:instance-1"),
			}),
			domain.NewInstance(domain.DirParams{
				Name:           "test.txt",
				Instance:       "instance-1",
				Ver:            domain.NewBIVersion(8, 119),
				Locator:        "",
				Exists:         true,
				Meta:           domain.NewBIObjectMeta(0, 0, "0.000000", "", "", "", "", "", 0, "", false),
				Tag:            "",
				Flags:          0,
				Pending:        false,
				VersionedEpoch: 2,
				IDX:            domain.NewBIIndex("test.txt-instance:instance-1"),
			}),
		},
	)
}
