package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type bucketLayoutCommand struct {
	ContainerName string `arg:"" help:"Running container name." name:"container-name"`
	BucketName    string `arg:"" help:"Bucket name."            name:"bucket-name"`
}

func (c *bucketLayoutCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	layout, err := service.BucketLayout(ctx, c.ContainerName, c.BucketName)
	if err != nil {
		return fmt.Errorf("read bucket layout: %w", err)
	}

	return writeBucketLayout(stdout, layout)
}

func writeBucketLayout(stdout io.Writer, layout *domain.Layout) error {
	_, err := fmt.Fprintf(stdout, "generation=%d\n", layout.Generation())
	if err != nil {
		return fmt.Errorf("write bucket layout: %w", err)
	}

	return nil
}
