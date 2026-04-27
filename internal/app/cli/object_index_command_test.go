package cli_test

import (
	"bytes"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/cli"
	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestWriteObjectIndexEntriesJSON_IncludesPendingForDirEntries(t *testing.T) {
	t.Parallel()

	// Arrange
	resp := newListBIByObjectResponse()

	var stdout bytes.Buffer

	// Act
	err := cli.WriteObjectIndexEntriesJSON(&stdout, resp)

	// Assert
	require.NoError(t, err)
	require.JSONEq(t, `{
		"container": "rgw",
		"bucket": "bucket-a",
		"object": "object-a",
		"shard_id": 7,
		"entries": [
			{
				"type": "olh",
				"idx": "olh-idx",
				"name": "object-a",
				"instance": "",
				"exists": true,
				"pending": true
			},
			{
				"type": "plain",
				"idx": "plain-idx",
				"name": "object-a",
				"instance": "",
				"exists": true,
				"pending": true,
				"flags": 0
			},
			{
				"type": "instance",
				"idx": "instance-idx",
				"name": "object-a",
				"instance": "ver-1",
				"exists": true,
				"pending": false,
				"flags": 0
			}
		]
	}`, stdout.String())
}

func newListBIByObjectResponse() *flow.ListBIByObjectResponse {
	return &flow.ListBIByObjectResponse{
		Container:  "rgw",
		Bucket:     "bucket-a",
		Object:     "object-a",
		ShardID:    7,
		EntryGroup: newEntryGroup(),
	}
}

func newEntryGroup() *domain.EntryGroup {
	return domain.NewEntryGroup(
		[]*domain.OLH{newOLHEntry()},
		[]*domain.Plain{newPlainEntry()},
		[]*domain.Instance{newInstanceEntry()},
	)
}

func newOLHEntry() *domain.OLH {
	return domain.NewOLH(domain.OLHParams{
		DeleteMarker: false,
		Epoch:        0,
		Exists:       true,
		Instance:     "",
		Name:         "object-a",
		PendingLog: []domain.PendingLogParams{
			{
				Key: 1,
				Val: []domain.PendingLogItemParams{
					{
						DeleteMarker: false,
						Epoch:        0,
						Instance:     "ver-pending",
						Name:         "object-a",
						Op:           "write",
						OpTag:        "op-tag-1",
					},
				},
			},
		},
		PendingRemoval: false,
		Tag:            "",
		IDX:            domain.NewBIIndex("olh-idx"),
	})
}

func newPlainEntry() *domain.Plain {
	return domain.NewPlain(newDirParams("plain-idx", "", true))
}

func newInstanceEntry() *domain.Instance {
	return domain.NewInstance(newDirParams("instance-idx", "ver-1", false))
}

func newDirParams(idx, instance string, pending bool) domain.DirParams {
	return domain.DirParams{
		AccountedSize:    0,
		Appendable:       false,
		Category:         0,
		ContentType:      "",
		ETag:             "",
		Exists:           true,
		Flags:            0,
		Instance:         instance,
		Locator:          "",
		MTime:            "",
		Name:             "object-a",
		Owner:            "",
		OwnerDisplayName: "",
		Pending:          pending,
		Size:             0,
		StorageClass:     "",
		Tag:              "",
		UserData:         "",
		Pool:             0,
		Epoch:            0,
		VersionedEpoch:   0,
		IDX:              domain.NewBIIndex(idx),
	}
}
