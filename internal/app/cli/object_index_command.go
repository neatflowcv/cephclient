package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type objectIndexCommand struct {
	Container string `arg:"" help:"Container name." name:"container"`
	Bucket    string `arg:"" help:"Bucket name."    name:"bucket"`
	Object    string `arg:"" help:"Object name."    name:"object"`
	Shard     int    `arg:"" help:"Shard ID."       name:"shard"`
}

func (c *objectIndexCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	biList, err := service.BIListByObject(ctx, c.Container, c.Bucket, c.Object, c.Shard)
	if err != nil {
		return fmt.Errorf("read bucket index list: %w", err)
	}

	return writeObjectBIList(stdout, biList)
}

func writeObjectBIList(stdout io.Writer, biList *domain.BIList) error {
	for _, entry := range biList.Entries() {
		line, err := formatBIEntry(entry)
		if err != nil {
			return err
		}

		_, err = fmt.Fprintln(stdout, line)
		if err != nil {
			return fmt.Errorf("write bucket index list: %w", err)
		}
	}

	return nil
}
