package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type objectInspectCommand struct {
	Container string `arg:"" help:"Container name." name:"container"`
	Bucket    string `arg:"" help:"Bucket name."    name:"bucket"`
	Object    string `arg:"" help:"Object name."    name:"object"`
}

func (c *objectInspectCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	result, err := service.InspectObject(ctx, flow.InspectObjectRequest{
		ContainerName: c.Container,
		BucketName:    c.Bucket,
		ObjectName:    c.Object,
	})
	if err != nil {
		return fmt.Errorf("inspect object: %w", err)
	}

	return writeObjectInspect(stdout, c.Container, c.Bucket, c.Object, result)
}

func writeObjectInspect(
	stdout io.Writer,
	containerName, bucketName, objectName string,
	result *flow.InspectObjectResponse,
) error {
	payload, err := newObjectInspectOutput(containerName, bucketName, objectName, result)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")

	err = encoder.Encode(payload)
	if err != nil {
		return fmt.Errorf("encode object inspect output: %w", err)
	}

	return nil
}

type objectInspectOutput struct {
	Container   string                 `json:"container"`
	Bucket      string                 `json:"bucket"`
	DataPool    string                 `json:"data_pool"`
	Marker      string                 `json:"marker"`
	Name        string                 `json:"name"`
	TotalShards int                    `json:"total_shards"`
	Shard       int                    `json:"shard"`
	Placeholder bool                   `json:"placeholder"`
	OLH         *objectInspectOLH      `json:"olh,omitempty"`
	Versions    []objectInspectVersion `json:"versions"`
}

type objectInspectOLH struct {
	IDX            string `json:"idx"`
	Instance       string `json:"instance"`
	Exists         bool   `json:"exists"`
	Epoch          int    `json:"epoch"`
	PendingRemoval bool   `json:"pending_removal"`
	DeleteMarker   bool   `json:"delete_marker"`
}

type objectInspectVersion struct {
	Version        string `json:"version"`
	Exists         bool   `json:"exists"`
	MTime          string `json:"mtime"`
	VersionedEpoch int    `json:"versioned_epoch"`
	PlainIDX       string `json:"plain_idx,omitempty"`
	InstanceIDX    string `json:"instance_idx,omitempty"`
	RawExists      bool   `json:"raw_exists"`
	RawObject      string `json:"raw_object"`
}

type objectInspectBIEntry struct {
	Type           string `json:"type"`
	IDX            string `json:"idx"`
	Instance       string `json:"instance"`
	Exists         bool   `json:"exists"`
	VersionedEpoch int    `json:"versioned_epoch,omitempty"`
	MTime          string `json:"mtime,omitempty"`
	Epoch          int    `json:"epoch,omitempty"`
	PendingRemoval bool   `json:"pending_removal,omitempty"`
	DeleteMarker   bool   `json:"delete_marker,omitempty"`
	Name           string `json:"-"`
}

func newObjectInspectOutput(
	containerName, bucketName, objectName string,
	result *flow.InspectObjectResponse,
) (*objectInspectOutput, error) {
	bucketIndex, err := newObjectInspectBIEntries(result.BIList())
	if err != nil {
		return nil, err
	}

	versions := newObjectInspectVersions(objectName, result)
	olh := newObjectInspectOLH(result.BIList())

	placeholder := hasPlaceholder(bucketIndex)

	return &objectInspectOutput{
		Container:   containerName,
		Bucket:      bucketName,
		DataPool:    result.DataPool(),
		Marker:      result.Marker(),
		Name:        objectName,
		TotalShards: result.TotalShards(),
		Shard:       result.ShardID(),
		Placeholder: placeholder,
		OLH:         olh,
		Versions:    versions,
	}, nil
}

func newObjectInspectOLH(biList *domain.BIList) *objectInspectOLH {
	for _, entry := range biList.Entries() {
		typed, ok := entry.(*domain.OLH)
		if !ok {
			continue
		}

		return &objectInspectOLH{
			IDX:            typed.IDX(),
			Instance:       typed.Instance(),
			Exists:         typed.Exists(),
			Epoch:          typed.Epoch(),
			PendingRemoval: typed.PendingRemoval(),
			DeleteMarker:   typed.DeleteMarker(),
		}
	}

	return nil
}

func newObjectInspectVersions(objectName string, result *flow.InspectObjectResponse) []objectInspectVersion {
	grouped := make(map[string]objectInspectVersion)

	var orderedKeys []string

	for _, entry := range result.BIList().Entries() {
		switch typed := entry.(type) {
		case *domain.Plain:
			if isPlaceholderPlainEntry(
				typed.IDX(),
				typed.Name(),
				typed.MTime(),
			) {
				continue
			}

			orderedKeys = upsertPlainObjectInspectVersion(grouped, orderedKeys, typed)
		case *domain.Instance:
			orderedKeys = upsertInstanceObjectInspectVersion(grouped, orderedKeys, typed)
		}
	}

	var versions []objectInspectVersion
	for _, key := range orderedKeys {
		versions = append(versions, grouped[key])
	}

	slices.SortFunc(versions, func(left, right objectInspectVersion) int {
		return compareString(left.MTime, right.MTime)
	})

	for index := range versions {
		rawExists, rawObject := findVersionRawObject(
			result.Marker(),
			objectName,
			versions[index].Version,
			result.RawObjects(),
		)
		versions[index].RawExists = rawExists
		versions[index].RawObject = rawObject
	}

	return versions
}

func upsertPlainObjectInspectVersion(
	grouped map[string]objectInspectVersion,
	orderedKeys []string,
	entry *domain.Plain,
) []string {
	version := entry.Instance()

	current, exists := grouped[version]
	if !exists {
		orderedKeys = append(orderedKeys, version)
	}

	grouped[version] = objectInspectVersion{
		Version:        version,
		Exists:         current.Exists || entry.Exists(),
		MTime:          firstNonEmpty(current.MTime, entry.MTime()),
		VersionedEpoch: firstNonZero(current.VersionedEpoch, entry.VersionedEpoch()),
		PlainIDX:       firstNonEmpty(current.PlainIDX, entry.IDX()),
		InstanceIDX:    current.InstanceIDX,
		RawExists:      false,
		RawObject:      "",
	}

	return orderedKeys
}

func upsertInstanceObjectInspectVersion(
	grouped map[string]objectInspectVersion,
	orderedKeys []string,
	entry *domain.Instance,
) []string {
	version := entry.Instance()

	current, exists := grouped[version]
	if !exists {
		orderedKeys = append(orderedKeys, version)
	}

	grouped[version] = objectInspectVersion{
		Version:        version,
		Exists:         current.Exists || entry.Exists(),
		MTime:          firstNonEmpty(current.MTime, entry.MTime()),
		VersionedEpoch: firstNonZero(current.VersionedEpoch, entry.VersionedEpoch()),
		PlainIDX:       current.PlainIDX,
		InstanceIDX:    firstNonEmpty(current.InstanceIDX, entry.IDX()),
		RawExists:      false,
		RawObject:      "",
	}

	return orderedKeys
}

func findVersionRawObject(
	marker, objectName, version string,
	rawObjects []*flow.RawObjectExistence,
) (bool, string) {
	expectedObject := marker + "__:" + version + "_" + objectName

	for _, rawObject := range rawObjects {
		if rawObject.Name().Kind() != "version" {
			continue
		}

		if rawObject.Name().Value() != expectedObject {
			continue
		}

		return rawObject.Exists(), rawObject.Name().Value()
	}

	return false, ""
}

func newObjectInspectBIEntries(biList *domain.BIList) ([]objectInspectBIEntry, error) {
	var entries []objectInspectBIEntry

	for _, entry := range biList.Entries() {
		switch typed := entry.(type) {
		case *domain.Plain:
			entries = append(entries, objectInspectBIEntry{
				Type:           typed.Type(),
				IDX:            typed.IDX(),
				Instance:       typed.Instance(),
				Exists:         typed.Exists(),
				VersionedEpoch: typed.VersionedEpoch(),
				MTime:          typed.MTime(),
				Epoch:          0,
				PendingRemoval: false,
				DeleteMarker:   false,
				Name:           typed.Name(),
			})
		case *domain.Instance:
			entries = append(entries, objectInspectBIEntry{
				Type:           typed.Type(),
				IDX:            typed.IDX(),
				Instance:       typed.Instance(),
				Exists:         typed.Exists(),
				VersionedEpoch: typed.VersionedEpoch(),
				MTime:          typed.MTime(),
				Epoch:          0,
				PendingRemoval: false,
				DeleteMarker:   false,
				Name:           typed.Name(),
			})
		case *domain.OLH:
			entries = append(entries, objectInspectBIEntry{
				Type:           typed.Type(),
				IDX:            typed.IDX(),
				Instance:       typed.Instance(),
				Exists:         typed.Exists(),
				VersionedEpoch: 0,
				MTime:          "",
				Epoch:          typed.Epoch(),
				PendingRemoval: typed.PendingRemoval(),
				DeleteMarker:   typed.DeleteMarker(),
				Name:           typed.Name(),
			})
		default:
			return nil, fmt.Errorf("%w: %T", errUnsupportedBIEntryFormat, entry)
		}
	}

	return entries, nil
}

func hasPlaceholder(entries []objectInspectBIEntry) bool {
	return slices.ContainsFunc(entries, isPlaceholderEntry)
}

func isPlaceholderEntry(entry objectInspectBIEntry) bool {
	if entry.Type != "plain" {
		return false
	}

	return isPlaceholderPlainEntry(entry.IDX, entry.Name, entry.MTime)
}

func isPlaceholderPlainEntry(idx, name, mtime string) bool {
	return idx == name && mtime == "0.000000"
}

func firstNonEmpty(current, next string) string {
	if current != "" {
		return current
	}

	return next
}

func firstNonZero(current, next int) int {
	if current != 0 {
		return current
	}

	return next
}

func compareString(left, right string) int {
	if left < right {
		return -1
	}

	if left > right {
		return 1
	}

	return 0
}
