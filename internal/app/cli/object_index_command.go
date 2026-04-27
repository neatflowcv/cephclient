package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type objectIndexCommand struct {
	Container   string `arg:""                    help:"Container name." name:"container"`
	Bucket      string `arg:""                    help:"Bucket name."    name:"bucket"`
	Object      string `arg:""                    help:"Object name."    name:"object"`
	Shard       *int   `help:"Shard ID."          name:"shard"`
	TotalShards *int   `help:"Total shard count." name:"total-shards"`
}

func (c *objectIndexCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	resp, err := service.ListBIByObject(ctx, flow.ListBIByObjectRequest{
		ContainerName: c.Container,
		BucketName:    c.Bucket,
		ObjectName:    c.Object,
		ShardID:       c.Shard,
		TotalShards:   c.TotalShards,
	})
	if err != nil {
		return fmt.Errorf("read bucket index list: %w", err)
	}

	return WriteObjectIndexEntriesJSON(stdout, resp)
}

func WriteObjectIndexEntriesJSON(stdout io.Writer, resp *flow.ListBIByObjectResponse) error {
	payload, err := newObjectIndexEntriesResponse(resp)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")

	err = encoder.Encode(payload)
	if err != nil {
		return fmt.Errorf("encode object index output: %w", err)
	}

	return nil
}

type objectIndexEntriesResponse struct {
	Container string                     `json:"container"`
	Bucket    string                     `json:"bucket"`
	Object    string                     `json:"object"`
	ShardID   int                        `json:"shard_id"`
	Entries   []objectIndexEntryResponse `json:"entries"`
}

type objectIndexEntryResponse interface {
	isObjectIndexEntryResponse()
}

type plainObjectIndexEntryResponse struct {
	Type           string `json:"type"`
	IDX            string `json:"idx"`
	Name           string `json:"name"`
	Instance       string `json:"instance"`
	Exists         bool   `json:"exists"`
	VersionedEpoch int    `json:"versioned_epoch,omitempty"`
	MTime          string `json:"mtime,omitempty"`
}

type instanceObjectIndexEntryResponse struct {
	Type           string `json:"type"`
	IDX            string `json:"idx"`
	Name           string `json:"name"`
	Instance       string `json:"instance"`
	Exists         bool   `json:"exists"`
	VersionedEpoch int    `json:"versioned_epoch,omitempty"`
	MTime          string `json:"mtime,omitempty"`
}

type olhObjectIndexEntryResponse struct {
	Type           string `json:"type"`
	IDX            string `json:"idx"`
	Name           string `json:"name"`
	Instance       string `json:"instance"`
	Exists         bool   `json:"exists"`
	Epoch          int    `json:"epoch,omitempty"`
	PendingRemoval bool   `json:"pending_removal,omitempty"`
	DeleteMarker   bool   `json:"delete_marker,omitempty"`
}

func (plainObjectIndexEntryResponse) isObjectIndexEntryResponse() {}

func (instanceObjectIndexEntryResponse) isObjectIndexEntryResponse() {}

func (olhObjectIndexEntryResponse) isObjectIndexEntryResponse() {}

func newObjectIndexEntriesResponse(resp *flow.ListBIByObjectResponse) (*objectIndexEntriesResponse, error) {
	var entries []objectIndexEntryResponse

	groupEntries, err := appendObjectIndexEntries(entries, resp.EntryGroup.OLHs())
	if err != nil {
		return nil, err
	}

	groupEntries, err = appendObjectIndexEntries(groupEntries, resp.EntryGroup.Plains())
	if err != nil {
		return nil, err
	}

	groupEntries, err = appendObjectIndexEntries(groupEntries, resp.EntryGroup.Instances())
	if err != nil {
		return nil, err
	}

	return &objectIndexEntriesResponse{
		Container: resp.Container,
		Bucket:    resp.Bucket,
		Object:    resp.Object,
		ShardID:   resp.ShardID,
		Entries:   groupEntries,
	}, nil
}

func appendObjectIndexEntries[T domain.BIEntry](
	items []objectIndexEntryResponse,
	entries []T,
) ([]objectIndexEntryResponse, error) {
	for _, entry := range entries {
		item, err := newObjectIndexEntryResponse(entry)
		if err != nil {
			return nil, err
		}

		items = append(items, item)
	}

	return items, nil
}

func newObjectIndexEntryResponse(entry domain.BIEntry) (objectIndexEntryResponse, error) {
	switch typed := entry.(type) {
	case *domain.Plain:
		return plainObjectIndexEntryResponse{
			Type:           typed.Type(),
			IDX:            typed.IDX(),
			Name:           typed.Name(),
			Instance:       typed.Instance(),
			Exists:         typed.Exists(),
			VersionedEpoch: typed.VersionedEpoch(),
			MTime:          typed.MTime(),
		}, nil
	case *domain.Instance:
		return instanceObjectIndexEntryResponse{
			Type:           typed.Type(),
			IDX:            typed.IDX(),
			Name:           typed.Name(),
			Instance:       typed.Instance(),
			Exists:         typed.Exists(),
			VersionedEpoch: typed.VersionedEpoch(),
			MTime:          typed.MTime(),
		}, nil
	case *domain.OLH:
		return olhObjectIndexEntryResponse{
			Type:           typed.Type(),
			IDX:            typed.IDX(),
			Name:           typed.Name(),
			Instance:       typed.Instance(),
			Exists:         typed.Exists(),
			Epoch:          typed.Epoch(),
			PendingRemoval: typed.PendingRemoval(),
			DeleteMarker:   typed.DeleteMarker(),
		}, nil
	default:
		return nil, fmt.Errorf("%w: %T", errUnsupportedBIEntryFormat, entry)
	}
}
