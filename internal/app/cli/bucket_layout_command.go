package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type bucketLayoutCommand struct {
	Container string `arg:"" help:"Container name." name:"container"`
	Bucket    string `arg:"" help:"Bucket name."    name:"bucket"`
}

func (c *bucketLayoutCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	layout, err := service.BucketLayout(ctx, c.Container, c.Bucket)
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
