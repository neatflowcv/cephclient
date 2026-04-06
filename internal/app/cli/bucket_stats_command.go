package cli

import (
	"context"
	"fmt"
	"io"
	"math"

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
		"id=%s\nname=%s\nsize=%s\nobject_count=%d\ntotal_shards=%d\nversioning=%s\nmarker=%s\n",
		stats.ID(),
		stats.Name(),
		formatBucketSize(stats.Size()),
		stats.ObjectCount(),
		stats.TotalShards(),
		stats.Versioning(),
		stats.Marker(),
	)
	if err != nil {
		return fmt.Errorf("write bucket stats: %w", err)
	}

	return nil
}

func formatBucketSize(size int64) string {
	const unit = 1024

	if size < unit {
		return fmt.Sprintf("%d B", size)
	}

	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	value := float64(size)

	exponent := min(int(math.Log(value)/math.Log(unit)), len(units))

	human := value / math.Pow(unit, float64(exponent))

	return fmt.Sprintf("%d B (%.2f %s)", size, human, units[exponent-1])
}
