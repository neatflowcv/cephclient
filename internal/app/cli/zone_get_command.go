package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type zoneGetCommand struct {
	ContainerName string `arg:"" help:"Running container name." name:"container-name"`
}

func (c *zoneGetCommand) Run(ctx context.Context, service *flow.Service, stdout io.Writer) error {
	zone, err := service.GetDefaultZone(ctx, c.ContainerName)
	if err != nil {
		return fmt.Errorf("read default zone: %w", err)
	}

	return writeZone(stdout, zone)
}

func writeZone(stdout io.Writer, zone *domain.Zone) error {
	_, err := fmt.Fprintf(stdout, "data_pool=%s\nindex_pool=%s\n", zone.DataPool(), zone.IndexPool())
	if err != nil {
		return fmt.Errorf("write default zone: %w", err)
	}

	return nil
}
