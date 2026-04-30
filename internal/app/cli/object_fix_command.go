package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

type objectFixCommand struct {
	TargetContainer    string `arg:"" help:"Container name to fix."        name:"target-container"`
	ReferenceContainer string `arg:"" help:"Container name to compare to." name:"reference-container"`
	Bucket             string `arg:"" help:"Bucket name."                  name:"bucket"`
	Object             string `arg:"" help:"Object name."                  name:"object"`
}

func (c *objectFixCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	err := service.FixObject(ctx, flow.FixObjectRequest{
		TargetContainerName:    c.TargetContainer,
		ReferenceContainerName: c.ReferenceContainer,
		BucketName:             c.Bucket,
		ObjectName:             c.Object,
	})
	if err != nil {
		return fmt.Errorf("fix object: %w", err)
	}

	return nil
}
