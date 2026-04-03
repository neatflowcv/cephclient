package cli

import (
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

func writeRMSupportCandidates(stdout io.Writer, entries []domain.BIEntry) error {
	for index, entry := range entries {
		line, err := formatBIEntry(entry)
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(stdout, "[%d] %s\n", index+1, line)
		if err != nil {
			return fmt.Errorf("write rm-support candidates: %w", err)
		}
	}

	return nil
}

func writeRMSupportConfirmation(stdout io.Writer, selections []rmSupportSelection) error {
	_, err := fmt.Fprintln(stdout, "selected entries:")
	if err != nil {
		return fmt.Errorf("write confirmation header: %w", err)
	}

	for _, selection := range selections {
		line, formatErr := formatBIEntry(selection.entry)
		if formatErr != nil {
			return formatErr
		}

		_, err = fmt.Fprintf(stdout, "[%d] %s\n", selection.number, line)
		if err != nil {
			return fmt.Errorf("write selected entry: %w", err)
		}
	}

	return nil
}

func writeRMSupportIDXList(stdout io.Writer, selections []rmSupportSelection) error {
	_, err := fmt.Fprintln(stdout, "confirmed idx values:")
	if err != nil {
		return fmt.Errorf("write idx header: %w", err)
	}

	for _, selection := range selections {
		_, err = fmt.Fprintf(stdout, "idx=%s\n", quoteField(selection.entry.IDX().Escaped()))
		if err != nil {
			return fmt.Errorf("write idx list: %w", err)
		}
	}

	return nil
}

func writeRMSupportCancelled(stdout io.Writer) error {
	_, err := fmt.Fprintln(stdout, "rm-support cancelled.")
	if err != nil {
		return fmt.Errorf("write cancellation message: %w", err)
	}

	return nil
}

func writeRMSupportOmapKeys(
	stdout io.Writer,
	phase, indexPool, marker string,
	shardID int,
	omapKeys []*domain.BIIndex,
) error {
	_, err := fmt.Fprintf(
		stdout,
		"omap keys %s: index_pool=%s marker=%s shard=%d\n",
		phase,
		indexPool,
		marker,
		shardID,
	)
	if err != nil {
		return fmt.Errorf("write omap header: %w", err)
	}

	return writeOmapKeys(stdout, omapKeys)
}
