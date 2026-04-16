package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

type objectPurgeCommand struct {
	Container string `arg:"" help:"Container name." name:"container"`
	Bucket    string `arg:"" help:"Bucket name."    name:"bucket"`
	Object    string `arg:"" help:"Object name."    name:"object"`
}

func (c *objectPurgeCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	err := service.PurgeObject(ctx, flow.PurgeObjectRequest{
		ContainerName: c.Container,
		BucketName:    c.Bucket,
		ObjectName:    c.Object,
	})
	if err != nil {
		return fmt.Errorf("purge object: %w", err)
	}

	_, err = fmt.Fprintf(
		stdout,
		"purged object=%s\n",
		quoteField(c.Object),
	)
	if err != nil {
		return fmt.Errorf("write object purge result: %w", err)
	}

	return nil
}
