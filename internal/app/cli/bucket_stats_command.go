package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type bucketStatsCommand struct {
	Container string `arg:"" help:"Container name." name:"container"`
	Bucket    string `arg:"" help:"Bucket name."    name:"bucket"`
}

func (c *bucketStatsCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	stats, err := service.BucketStats(ctx, c.Container, c.Bucket)
	if err != nil {
		return fmt.Errorf("read bucket stats: %w", err)
	}

	return writeBucketStats(stdout, stats)
}

func writeBucketStats(stdout io.Writer, stats *domain.BucketStats) error {
	_, err := fmt.Fprintf(
		stdout,
		"id=%s\ntotal_shards=%d\nversioning=%s\nmarker=%s\n",
		stats.ID(),
		stats.TotalShards(),
		stats.Versioning(),
		stats.Marker(),
	)
	if err != nil {
		return fmt.Errorf("write bucket stats: %w", err)
	}

	return nil
}
