package podman

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

var errUnsupportedBIEntryType = errors.New("unsupported bi entry type")

type biListResponse []biListEntryResponse

func (r biListResponse) toDomain() (*domain.BIList, error) {
	entries := make([]domain.BIEntry, 0, len(r))

	for _, item := range r {
		entry, err := item.toDomain()
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	return domain.NewBIList(entries), nil
}

type biListEntryResponse struct {
	Entry json.RawMessage `json:"entry"`
	IDX   string          `json:"idx"`
	Type  string          `json:"type"`
}

func (r biListEntryResponse) toDomain() (domain.BIEntry, error) {
	idx := domain.NewBIIndex(r.IDX)

	switch r.Type {
	case "plain":
		entry, err := r.decodeObjectEntry()
		if err != nil {
			return nil, fmt.Errorf("decode plain bi entry: %w", err)
		}

		return domain.NewPlain(entry.toDirParams(idx)), nil
	case "instance":
		entry, err := r.decodeObjectEntry()
		if err != nil {
			return nil, fmt.Errorf("decode instance bi entry: %w", err)
		}

		return domain.NewInstance(entry.toDirParams(idx)), nil
	case "olh":
		var entry biOLHEntryResponse

		err := json.Unmarshal(r.Entry, &entry)
		if err != nil {
			return nil, fmt.Errorf("decode olh bi entry: %w", err)
		}

		var pendingLog []domain.PendingLogParams
		for _, item := range entry.PendingLog {
			pendingLog = append(pendingLog, item.toDomain())
		}

		return domain.NewOLH(domain.OLHParams{
			DeleteMarker:   entry.DeleteMarker,
			Epoch:          entry.Epoch,
			Exists:         entry.Exists,
			Instance:       entry.Key.Instance,
			Name:           entry.Key.Name,
			PendingLog:     pendingLog,
			PendingRemoval: entry.PendingRemoval,
			Tag:            entry.Tag,
			IDX:            idx,
		}), nil
	default:
		return nil, fmt.Errorf("%w: %s", errUnsupportedBIEntryType, r.Type)
	}
}

func (r biListEntryResponse) decodeObjectEntry() (biObjectEntryResponse, error) {
	var entry biObjectEntryResponse

	err := json.Unmarshal(r.Entry, &entry)
	if err != nil {
		return biObjectEntryResponse{}, fmt.Errorf("unmarshal object entry: %w", err)
	}

	return entry, nil
}

type biObjectEntryResponse struct {
	Exists         bool                 `json:"exists"`
	Flags          int                  `json:"flags"`
	Instance       string               `json:"instance"`
	Locator        string               `json:"locator"`
	Meta           biObjectMetaResponse `json:"meta"`
	Name           string               `json:"name"`
	PendingMap     []json.RawMessage    `json:"pending_map"`
	Tag            string               `json:"tag"`
	Ver            biVersionResponse    `json:"ver"`
	VersionedEpoch int                  `json:"versioned_epoch"`
}

func (r biObjectEntryResponse) toDirParams(idx *domain.BIIndex) domain.DirParams {
	return domain.DirParams{
		AccountedSize:    r.Meta.AccountedSize,
		Appendable:       r.Meta.Appendable,
		Category:         r.Meta.Category,
		ContentType:      r.Meta.ContentType,
		ETag:             r.Meta.ETag,
		Exists:           r.Exists,
		Flags:            r.Flags,
		Instance:         r.Instance,
		Locator:          r.Locator,
		MTime:            r.Meta.MTime,
		Name:             r.Name,
		Owner:            r.Meta.Owner,
		OwnerDisplayName: r.Meta.OwnerDisplayName,
		Pending:          len(r.PendingMap) > 0,
		Size:             r.Meta.Size,
		StorageClass:     r.Meta.StorageClass,
		Tag:              r.Tag,
		UserData:         r.Meta.UserData,
		Ver:              r.Ver.toDomain(),
		VersionedEpoch:   r.VersionedEpoch,
		IDX:              idx,
	}
}

type biObjectMetaResponse struct {
	AccountedSize    int    `json:"accounted_size"`
	Appendable       bool   `json:"appendable"`
	Category         int    `json:"category"`
	ContentType      string `json:"content_type"`
	ETag             string `json:"etag"`
	MTime            string `json:"mtime"`
	Owner            string `json:"owner"`
	OwnerDisplayName string `json:"owner_display_name"`
	Size             int    `json:"size"`
	StorageClass     string `json:"storage_class"`
	UserData         string `json:"user_data"`
}

type biVersionResponse struct {
	Epoch int `json:"epoch"`
	Pool  int `json:"pool"`
}

func (r biVersionResponse) toDomain() *domain.BIVersion {
	return domain.NewBIVersion(r.Pool, r.Epoch)
}

type biOLHEntryResponse struct {
	DeleteMarker   bool                        `json:"delete_marker"`
	Epoch          int                         `json:"epoch"`
	Exists         bool                        `json:"exists"`
	Key            biOLHKeyResponse            `json:"key"`
	PendingLog     []biPendingLogEntryResponse `json:"pending_log"`
	PendingRemoval bool                        `json:"pending_removal"`
	Tag            string                      `json:"tag"`
}

type biOLHKeyResponse struct {
	Instance string `json:"instance"`
	Name     string `json:"name"`
}

type biPendingLogEntryResponse struct {
	Key int                        `json:"key"`
	Val []biPendingLogItemResponse `json:"val"`
}

func (r biPendingLogEntryResponse) toDomain() domain.PendingLogParams {
	var items []domain.PendingLogItemParams
	for _, item := range r.Val {
		items = append(items, item.toDomain())
	}

	return domain.PendingLogParams{
		Key: r.Key,
		Val: items,
	}
}

type biPendingLogItemResponse struct {
	DeleteMarker bool             `json:"delete_marker"`
	Epoch        int              `json:"epoch"`
	Key          biOLHKeyResponse `json:"key"`
	Op           string           `json:"op"`
	OpTag        string           `json:"op_tag"`
}

func (r biPendingLogItemResponse) toDomain() domain.PendingLogItemParams {
	return domain.PendingLogItemParams{
		DeleteMarker: r.DeleteMarker,
		Epoch:        r.Epoch,
		Instance:     r.Key.Instance,
		Name:         r.Key.Name,
		Op:           r.Op,
		OpTag:        r.OpTag,
	}
}
