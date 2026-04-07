package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

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

	return writeBucketIndexEntries(stdout, biList)
}
