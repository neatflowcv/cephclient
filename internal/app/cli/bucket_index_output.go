package cli

import (
	"errors"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

var errUnsupportedBIEntryFormat = errors.New("format bucket index entry: unsupported entry type")

func writeBucketIndexEntries(stdout io.Writer, biList *domain.BIList) error {
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
	case *domain.Plain:
		return fmt.Sprintf(
			"type=%s idx=%s name=%s instance=%s exists=%t versioned_epoch=%d mtime=%s",
			typed.Type(),
			quoteField(typed.IDX()),
			quoteField(typed.Entry().Name()),
			quoteField(typed.Entry().Instance()),
			typed.Entry().Exists(),
			typed.Entry().VersionedEpoch(),
			quoteField(formatObjectMTime(typed.Entry())),
		), nil
	case *domain.Instance:
		return fmt.Sprintf(
			"type=%s idx=%s name=%s instance=%s exists=%t versioned_epoch=%d mtime=%s",
			typed.Type(),
			quoteField(typed.IDX()),
			quoteField(typed.Entry().Name()),
			quoteField(typed.Entry().Instance()),
			typed.Entry().Exists(),
			typed.Entry().VersionedEpoch(),
			quoteField(formatObjectMTime(typed.Entry())),
		), nil
	case *domain.OLH:
		return fmt.Sprintf(
			"type=%s idx=%s name=%s instance=%s exists=%t epoch=%d pending_removal=%t delete_marker=%t",
			typed.Type(),
			quoteField(typed.IDX()),
			quoteField(typed.Name()),
			quoteField(typed.Instance()),
			typed.Exists(),
			typed.Epoch(),
			typed.PendingRemoval(),
			typed.DeleteMarker(),
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
