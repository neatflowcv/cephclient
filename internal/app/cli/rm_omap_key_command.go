package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

type rmOmapKeyCommand struct {
	ContainerName string `arg:"" help:"Running container name." name:"container-name"`
	IndexPool     string `arg:"" help:"Index pool name."        name:"index-pool"`
	Marker        string `arg:"" help:"Bucket marker."          name:"marker"`
	Shard         int    `arg:"" help:"Shard ID."               name:"shard"`
	Key           string `arg:"" help:"OMAP key to remove."     name:"key"`
}

func (c *rmOmapKeyCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	err := service.RemoveOmapKey(ctx, c.ContainerName, c.IndexPool, c.Marker, c.Shard, c.Key)
	if err != nil {
		return fmt.Errorf("remove omap key: %w", err)
	}

	_, err = fmt.Fprintf(stdout, "removed idx=%s\n", quoteField(c.Key))
	if err != nil {
		return fmt.Errorf("write remove omap key result: %w", err)
	}

	return nil
}
