package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type bucketStatsCommand struct {
	ContainerName string `arg:"" help:"Running container name." name:"container-name"`
	BucketName    string `arg:"" help:"Bucket name."            name:"bucket-name"`
}

func (c *bucketStatsCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	stats, err := service.BucketStats(ctx, c.ContainerName, c.BucketName)
	if err != nil {
		return fmt.Errorf("read bucket stats: %w", err)
	}

	return writeBucketStats(stdout, stats)
}

func writeBucketStats(stdout io.Writer, stats *domain.BucketStats) error {
	_, err := fmt.Fprintln(stdout, stats.ID())
	if err != nil {
		return fmt.Errorf("write bucket stats: %w", err)
	}

	return nil
}
