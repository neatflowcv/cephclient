package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

type objectIndexCommand struct {
	Container string `arg:"" help:"Container name." name:"container"`
	Bucket    string `arg:"" help:"Bucket name."    name:"bucket"`
	Object    string `arg:"" help:"Object name."    name:"object"`
	Shard     int    `arg:"" help:"Shard ID."       name:"shard"`
}

func (c *objectIndexCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	resp, err := service.ListBIByObject(ctx, flow.ListBIByObjectRequest{
		ContainerName: c.Container,
		BucketName:    c.Bucket,
		ObjectName:    c.Object,
		ShardID:       c.Shard,
	})
	if err != nil {
		return fmt.Errorf("read bucket index list: %w", err)
	}

	return writeBucketIndexEntries(stdout, resp.BIList())
}
