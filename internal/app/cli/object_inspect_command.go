package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
)

type objectInspectCommand struct {
	Container string `arg:"" help:"Container name." name:"container"`
	Bucket    string `arg:"" help:"Bucket name."    name:"bucket"`
	Object    string `arg:"" help:"Object name."    name:"object"`
}

func (c *objectInspectCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	result, err := service.InspectObject(ctx, c.Container, c.Bucket, c.Object)
	if err != nil {
		return fmt.Errorf("inspect object: %w", err)
	}

	return writeObjectInspect(stdout, result)
}

func writeObjectInspect(stdout io.Writer, result *flow.ObjectInspectResult) error {
	_, err := fmt.Fprintf(
		stdout,
		"data_pool=%s\nmarker=%s\ntotal_shards=%d\nshard=%d\n",
		result.DataPool(),
		result.Marker(),
		result.TotalShards(),
		result.ShardID(),
	)
	if err != nil {
		return fmt.Errorf("write object inspect header: %w", err)
	}

	_, err = fmt.Fprintln(stdout, "bucket_index:")
	if err != nil {
		return fmt.Errorf("write object inspect bucket index header: %w", err)
	}

	err = writeBucketIndexEntries(stdout, result.BIList())
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(stdout, "raw_objects:")
	if err != nil {
		return fmt.Errorf("write object inspect raw objects header: %w", err)
	}

	for _, rawObject := range result.RawObjects() {
		_, err = fmt.Fprintf(
			stdout,
			"type=%s exists=%t object=%s\n",
			rawObject.Name().Kind(),
			rawObject.Exists(),
			quoteField(rawObject.Name().Value()),
		)
		if err != nil {
			return fmt.Errorf("write object inspect raw object: %w", err)
		}
	}

	return nil
}
