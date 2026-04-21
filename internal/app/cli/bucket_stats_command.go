package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

type bucketStatsCommand struct {
	Container string `arg:"" help:"Container name." name:"container"`
	Bucket    string `arg:"" help:"Bucket name."    name:"bucket"`
}

func (c *bucketStatsCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	stats, err := service.GetBucketStats(ctx, flow.GetBucketStatsRequest{
		ContainerName: c.Container,
		BucketName:    c.Bucket,
	})
	if err != nil {
		return fmt.Errorf("read bucket stats: %w", err)
	}

	return WriteBucketStats(stdout, stats)
}

func WriteBucketStats(stdout io.Writer, stats *flow.GetBucketStatsResponse) error {
	payload := bucketStatsOutput{
		Container:   stats.ContainerName,
		Bucket:      stats.BucketName,
		ID:          stats.ID,
		Size:        stats.Size,
		SizeHuman:   formatBucketSize(stats.Size),
		ObjectCount: stats.ObjectCount,
		TotalShards: stats.TotalShards,
		Versioning:  string(stats.Versioning),
		Marker:      stats.Marker,
	}

	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("write bucket stats: %w", err)
	}

	_, err = fmt.Fprintln(stdout, string(encoded))
	if err != nil {
		return fmt.Errorf("write bucket stats: %w", err)
	}

	return nil
}

type bucketStatsOutput struct {
	Container   string `json:"container"`
	Bucket      string `json:"bucket"`
	ID          string `json:"id"`
	Size        int64  `json:"size"`
	SizeHuman   string `json:"size_human"`
	ObjectCount int    `json:"object_count"`
	TotalShards int    `json:"total_shards"`
	Versioning  string `json:"versioning"`
	Marker      string `json:"marker"`
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

	return fmt.Sprintf("%.2f %s", human, units[exponent-1])
}
