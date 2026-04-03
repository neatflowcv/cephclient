package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type objectShardCommand struct {
	ContainerName string `arg:"" help:"Running container name." name:"container-name"`
	ObjectName    string `arg:"" help:"Object name."            name:"object"`
	TotalShards   int    `arg:"" help:"Total shard count."      name:"total-shards"`
}

func (c *objectShardCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	shard, err := service.ObjectShard(ctx, c.ContainerName, c.ObjectName, c.TotalShards)
	if err != nil {
		return fmt.Errorf("read object shard: %w", err)
	}

	return writeObjectShard(stdout, shard)
}

func writeObjectShard(stdout io.Writer, shard *domain.ObjectShard) error {
	_, err := fmt.Fprintf(stdout, "shard=%d\n", shard.Shard())
	if err != nil {
		return fmt.Errorf("write object shard: %w", err)
	}

	return nil
}
