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
				domain.NewInstance(newTestDirParams("instance-1", 119, "test.txt-instance:instance-1")),
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
			domain.NewPlain(newTestDirParams("instance-2", 120, "test.txt:instance-2")),
			domain.NewPlain(newTestDirParams("instance-1", 119, "test.txt:instance-1")),
		},
		[]*domain.Instance{
			domain.NewInstance(newTestDirParams("instance-1", 119, "test.txt-instance:instance-1")),
			domain.NewInstance(newTestDirParams("instance-1", 119, "test.txt-instance:instance-1")),
		},
	)
}

func newTestDirParams(instance string, epoch int, idx string) domain.DirParams {
	return domain.DirParams{
		Name:             "test.txt",
		Instance:         instance,
		Pool:             8,
		Epoch:            epoch,
		Locator:          "",
		Exists:           true,
		Category:         0,
		Size:             0,
		MTime:            "0.000000",
		ETag:             "",
		StorageClass:     "",
		Owner:            "",
		OwnerDisplayName: "",
		ContentType:      "",
		AccountedSize:    0,
		UserData:         "",
		Appendable:       false,
		Tag:              "",
		Flags:            0,
		Pending:          false,
		VersionedEpoch:   2,
		IDX:              domain.NewBIIndex(idx),
	}
}
