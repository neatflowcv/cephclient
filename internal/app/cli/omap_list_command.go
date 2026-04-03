package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type omapListCommand struct {
	ContainerName string `arg:"" help:"Running container name." name:"container-name"`
	IndexPool     string `arg:"" help:"Index pool name."        name:"index-pool"`
	Marker        string `arg:"" help:"Bucket marker."          name:"marker"`
	Shard         int    `arg:"" help:"Shard ID."               name:"shard"`
}

func (c *omapListCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	indexes, err := service.ListOmapKeys(ctx, c.ContainerName, c.IndexPool, c.Marker, c.Shard)
	if err != nil {
		return fmt.Errorf("list omap keys: %w", err)
	}

	return writeOmapKeys(stdout, indexes)
}

func writeOmapKeys(stdout io.Writer, indexes []*domain.BIIndex) error {
	for _, index := range indexes {
		_, err := fmt.Fprintf(stdout, "idx=%s\n", quoteField(index.Escaped()))
		if err != nil {
			return fmt.Errorf("write omap keys: %w", err)
		}
	}

	return nil
}
