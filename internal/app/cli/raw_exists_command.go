package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

type rawExistsCommand struct {
	Container string `arg:"" help:"Container name."  name:"container"`
	Pool      string `arg:"" help:"Pool name."       name:"pool"`
	Object    string `arg:"" help:"Raw object name." name:"object"`
}

func (c *rawExistsCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	exists, err := service.HasRawObject(ctx, c.Container, c.Pool, c.Object)
	if err != nil {
		return fmt.Errorf("check raw object existence: %w", err)
	}

	_, err = fmt.Fprintf(
		stdout,
		"exists=%t object=%s\n",
		exists,
		quoteField(c.Object),
	)
	if err != nil {
		return fmt.Errorf("write raw exists result: %w", err)
	}

	return nil
}
