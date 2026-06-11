package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

type objectRepairCommand struct {
	MasterContainer    string `arg:"" help:"Master container name."    name:"master-container"`
	SecondaryContainer string `arg:"" help:"Secondary container name." name:"secondary-container"`
	Bucket             string `arg:"" help:"Bucket name."              name:"bucket"`
	Object             string `arg:"" help:"Object name."              name:"object"`
}

func (c *objectRepairCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	result, err := service.RepairObject(ctx, flow.RepairObjectRequest{
		MasterContainerName:    c.MasterContainer,
		SecondaryContainerName: c.SecondaryContainer,
		BucketName:             c.Bucket,
		ObjectName:             c.Object,
	})
	if err != nil {
		return fmt.Errorf("repair object: %w", err)
	}

	_, err = fmt.Fprintf(
		stdout,
		"repaired object=%s secondary_omap=%d master_omap=%d\n",
		quoteField(c.Object),
		len(result.SecondaryRemovedOmapKeys()),
		len(result.MasterRemovedOmapKeys()),
	)
	if err != nil {
		return fmt.Errorf("write object repair result: %w", err)
	}

	err = writeObjectRepairResult(stdout, result)
	if err != nil {
		return err
	}

	return nil
}

func writeObjectRepairResult(stdout io.Writer, result *flow.RepairObjectResponse) error {
	for _, key := range result.SecondaryRemovedOmapKeys() {
		_, err := fmt.Fprintf(stdout, "removed secondary omap_key=%s\n", quoteField(key))
		if err != nil {
			return fmt.Errorf("write secondary omap repair result: %w", err)
		}
	}

	for _, key := range result.MasterRemovedOmapKeys() {
		_, err := fmt.Fprintf(stdout, "removed master omap_key=%s\n", quoteField(key))
		if err != nil {
			return fmt.Errorf("write master omap repair result: %w", err)
		}
	}

	return nil
}
