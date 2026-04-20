package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

type rawRmCommand struct {
	Container string `arg:"" help:"Container name."  name:"container"`
	Pool      string `arg:"" help:"Pool name."       name:"pool"`
	Object    string `arg:"" help:"Raw object name." name:"object"`
}

func (c *rawRmCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	err := service.RemoveRawObject(ctx, c.Container, c.Pool, c.Object)
	if err != nil {
		return fmt.Errorf("remove raw object: %w", err)
	}

	_, err = fmt.Fprintf(stdout, "removed object=%s\n", quoteField(c.Object))
	if err != nil {
		return fmt.Errorf("write raw rm result: %w", err)
	}

	return nil
}
