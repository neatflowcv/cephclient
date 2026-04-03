package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

type bucketListCommand struct {
	ContainerName string `arg:"" help:"Running container name." name:"container-name"`
}

func (c *bucketListCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	buckets, err := service.ListBuckets(ctx, c.ContainerName)
	if err != nil {
		return fmt.Errorf("list buckets: %w", err)
	}

	return writeBucketList(stdout, buckets)
}

func writeBucketList(stdout io.Writer, buckets []string) error {
	for _, bucket := range buckets {
		_, err := fmt.Fprintln(stdout, bucket)
		if err != nil {
			return fmt.Errorf("write bucket list: %w", err)
		}
	}

	return nil
}
