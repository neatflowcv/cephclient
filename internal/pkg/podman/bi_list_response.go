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

//nolint:ireturn // BI entry decoding intentionally returns the domain interface implemented by multiple entry types.
func (r biListEntryResponse) toDomain() (domain.BIEntry, error) {
	idx := domain.NewBIIndex(r.IDX)

	switch r.Type {
	case "plain":
		var entry biObjectEntryResponse

		err := json.Unmarshal(r.Entry, &entry)
		if err != nil {
			return nil, fmt.Errorf("decode plain bi entry: %w", err)
		}

		return domain.NewPlainBIEntry(idx, entry.toDomain()), nil
	case "instance":
		var entry biObjectEntryResponse

		err := json.Unmarshal(r.Entry, &entry)
		if err != nil {
			return nil, fmt.Errorf("decode instance bi entry: %w", err)
		}

		return domain.NewInstanceBIEntry(idx, entry.toDomain()), nil
	case "olh":
		var entry biOLHEntryResponse

		err := json.Unmarshal(r.Entry, &entry)
		if err != nil {
			return nil, fmt.Errorf("decode olh bi entry: %w", err)
		}

		return domain.NewOLHBIEntry(idx, entry.toDomain()), nil
	default:
		return nil, fmt.Errorf("%w: %s", errUnsupportedBIEntryType, r.Type)
	}
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

func (r biObjectEntryResponse) toDomain() *domain.BIObjectEntry {
	return domain.NewBIObjectEntry(
		r.Name,
		r.Instance,
		r.Ver.toDomain(),
		r.Locator,
		r.Exists,
		r.Meta.toDomain(),
		r.Tag,
		r.Flags,
		len(r.PendingMap) > 0,
		r.VersionedEpoch,
	)
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

func (r biObjectMetaResponse) toDomain() *domain.BIObjectMeta {
	return domain.NewBIObjectMeta(
		r.Category,
		r.Size,
		r.MTime,
		r.ETag,
		r.StorageClass,
		r.Owner,
		r.OwnerDisplayName,
		r.ContentType,
		r.AccountedSize,
		r.UserData,
		r.Appendable,
	)
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

func (r biOLHEntryResponse) toDomain() *domain.BIOLHEntry {
	pendingLog := make([]domain.BIPendingLogEntry, 0, len(r.PendingLog))
	for _, item := range r.PendingLog {
		pendingLog = append(pendingLog, item.toDomain())
	}

	return domain.NewBIOLHEntry(
		r.Key.toDomain(),
		r.DeleteMarker,
		r.Epoch,
		pendingLog,
		r.Tag,
		r.Exists,
		r.PendingRemoval,
	)
}

type biOLHKeyResponse struct {
	Instance string `json:"instance"`
	Name     string `json:"name"`
}

func (r biOLHKeyResponse) toDomain() *domain.BIOLHKey {
	return domain.NewBIOLHKey(r.Name, r.Instance)
}

type biPendingLogEntryResponse struct {
	Key int                        `json:"key"`
	Val []biPendingLogItemResponse `json:"val"`
}

func (r biPendingLogEntryResponse) toDomain() domain.BIPendingLogEntry {
	items := make([]domain.BIPendingLogItem, 0, len(r.Val))
	for _, item := range r.Val {
		items = append(items, item.toDomain())
	}

	return domain.NewBIPendingLogEntry(r.Key, items)
}

type biPendingLogItemResponse struct {
	DeleteMarker bool             `json:"delete_marker"`
	Epoch        int              `json:"epoch"`
	Key          biOLHKeyResponse `json:"key"`
	Op           string           `json:"op"`
	OpTag        string           `json:"op_tag"`
}

func (r biPendingLogItemResponse) toDomain() domain.BIPendingLogItem {
	return domain.NewBIPendingLogItem(r.Epoch, r.Op, r.OpTag, r.Key.toDomain(), r.DeleteMarker)
}
