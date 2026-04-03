package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

type objectRmCommand struct {
	Container string `arg:"" help:"Container name."    name:"container"`
	Bucket    string `arg:"" help:"Bucket name."       name:"bucket"`
	Object    string `arg:"" help:"Object name."       name:"object"`
	Version   string `arg:"" help:"Object version ID." name:"version"`
}

func (c *objectRmCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	err := service.RemoveObject(ctx, c.Container, c.Bucket, c.Object, c.Version)
	if err != nil {
		return fmt.Errorf("remove object: %w", err)
	}

	_, err = fmt.Fprintf(
		stdout,
		"removed object=%s version=%s\n",
		quoteField(c.Object),
		quoteField(c.Version),
	)
	if err != nil {
		return fmt.Errorf("write object rm result: %w", err)
	}

	return nil
}
