package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

var errUnsupportedBIEntryFormat = errors.New("format bucket index entry: unsupported entry type")

type bucketIndexCommand struct {
	Container string `arg:"" help:"Container name." name:"container"`
	Bucket    string `arg:"" help:"Bucket name."    name:"bucket"`
	Shard     int    `arg:"" help:"Shard ID."       name:"shard"`
}

func (c *bucketIndexCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	biList, err := service.BIListByShard(ctx, c.Container, c.Bucket, c.Shard)
	if err != nil {
		return fmt.Errorf("read bucket index list: %w", err)
	}

	return writeBIList(stdout, biList)
}

func writeBIList(stdout io.Writer, biList *domain.BIList) error {
	for _, entry := range biList.Entries() {
		line, err := formatBIEntry(entry)
		if err != nil {
			return err
		}

		_, err = fmt.Fprintln(stdout, line)
		if err != nil {
			return fmt.Errorf("write bucket index list: %w", err)
		}
	}

	return nil
}

func formatBIEntry(entry domain.BIEntry) (string, error) {
	switch typed := entry.(type) {
	case *domain.PlainBIEntry:
		return fmt.Sprintf(
			"type=%s idx=%s name=%s instance=%s exists=%t versioned_epoch=%d mtime=%s",
			typed.Type(),
			quoteField(typed.IDX().Escaped()),
			quoteField(typed.Entry().Name()),
			quoteField(typed.Entry().Instance()),
			typed.Entry().Exists(),
			typed.Entry().VersionedEpoch(),
			quoteField(formatObjectMTime(typed.Entry())),
		), nil
	case *domain.InstanceBIEntry:
		return fmt.Sprintf(
			"type=%s idx=%s name=%s instance=%s exists=%t versioned_epoch=%d mtime=%s",
			typed.Type(),
			quoteField(typed.IDX().Escaped()),
			quoteField(typed.Entry().Name()),
			quoteField(typed.Entry().Instance()),
			typed.Entry().Exists(),
			typed.Entry().VersionedEpoch(),
			quoteField(formatObjectMTime(typed.Entry())),
		), nil
	case *domain.OLHBIEntry:
		return fmt.Sprintf(
			"type=%s idx=%s name=%s instance=%s exists=%t epoch=%d pending_removal=%t delete_marker=%t",
			typed.Type(),
			quoteField(typed.IDX().Escaped()),
			quoteField(typed.Entry().Key().Name()),
			quoteField(typed.Entry().Key().Instance()),
			typed.Entry().Exists(),
			typed.Entry().Epoch(),
			typed.Entry().PendingRemoval(),
			typed.Entry().DeleteMarker(),
		), nil
	default:
		return "", fmt.Errorf("%w: %T", errUnsupportedBIEntryFormat, entry)
	}
}

func formatObjectMTime(entry *domain.BIObjectEntry) string {
	if entry.Meta() == nil {
		return ""
	}

	return entry.Meta().MTime()
}
