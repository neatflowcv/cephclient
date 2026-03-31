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

type biListCommand struct {
	ContainerName string `arg:"" help:"Running container name." name:"container-name"`
	BucketName    string `arg:"" help:"Bucket name."            name:"bucket"`
	ObjectName    string `arg:"" help:"Object name."            name:"object"`
	ShardID       int    `arg:"" help:"Shard ID."               name:"shard-id"`
}

func (c *biListCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	biList, err := service.BIList(ctx, c.ContainerName, c.BucketName, c.ObjectName, c.ShardID)
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
			"type=%s idx=%s name=%s instance=%s exists=%t versioned_epoch=%d",
			typed.Type(),
			typed.IDX().Escaped(),
			typed.Entry().Name(),
			typed.Entry().Instance(),
			typed.Entry().Exists(),
			typed.Entry().VersionedEpoch(),
		), nil
	case *domain.InstanceBIEntry:
		return fmt.Sprintf(
			"type=%s idx=%s name=%s instance=%s exists=%t versioned_epoch=%d",
			typed.Type(),
			typed.IDX().Escaped(),
			typed.Entry().Name(),
			typed.Entry().Instance(),
			typed.Entry().Exists(),
			typed.Entry().VersionedEpoch(),
		), nil
	case *domain.OLHBIEntry:
		return fmt.Sprintf(
			"type=%s idx=%s name=%s instance=%s exists=%t epoch=%d pending_removal=%t delete_marker=%t",
			typed.Type(),
			typed.IDX().Escaped(),
			typed.Entry().Key().Name(),
			typed.Entry().Key().Instance(),
			typed.Entry().Exists(),
			typed.Entry().Epoch(),
			typed.Entry().PendingRemoval(),
			typed.Entry().DeleteMarker(),
		), nil
	default:
		return "", fmt.Errorf("%w: %T", errUnsupportedBIEntryFormat, entry)
	}
}
