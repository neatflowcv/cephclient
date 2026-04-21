package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

type omapListCommand struct {
	Container string `arg:"" help:"Container name."  name:"container"`
	IndexPool string `arg:"" help:"Index pool name." name:"index-pool"`
	Marker    string `arg:"" help:"Bucket marker."   name:"marker"`
	Shard     int    `arg:"" help:"Shard ID."        name:"shard"`
}

func (c *omapListCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	resp, err := service.ListOmapKeys(ctx, flow.ListOmapKeysRequest{
		ContainerName: c.Container,
		IndexPool:     c.IndexPool,
		Marker:        c.Marker,
		ShardID:       c.Shard,
	})
	if err != nil {
		return fmt.Errorf("list omap keys: %w", err)
	}

	return writeOmapKeys(stdout, resp.OmapKeys)
}

func writeOmapKeys(stdout io.Writer, indexes []string) error {
	for _, index := range indexes {
		_, err := fmt.Fprintf(stdout, "idx=%s\n", quoteField(index))
		if err != nil {
			return fmt.Errorf("write omap keys: %w", err)
		}
	}

	return nil
}
