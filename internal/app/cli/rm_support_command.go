package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

var errRMSupportNoBIEntries = errors.New("no BI entries found for the requested object")

type rmSupportCommand struct {
	ContainerName string `arg:""                                             help:"Running container name." name:"container-name"` //nolint:lll
	BucketName    string `arg:""                                             help:"Bucket name."            name:"bucket"`
	ObjectName    string `arg:""                                             help:"Object name."            name:"object"`
	ShowOmap      bool   `help:"Show OMAP keys for before/after comparison." name:"show-omap"`
}

func (c *rmSupportCommand) Run(
	ctx context.Context,
	service *flow.Service,
	stdin io.Reader,
	stdout io.Writer,
) error {
	plan, err := service.RMSupportPlan(ctx, c.ContainerName, c.BucketName, c.ObjectName, c.ShowOmap)
	if err != nil {
		return fmt.Errorf("prepare rm-support flow: %w", err)
	}

	entries := plan.BIList().Entries()
	if len(entries) == 0 {
		return errRMSupportNoBIEntries
	}

	reader := bufio.NewReader(stdin)

	err = writeRMSupportCandidates(stdout, entries)
	if err != nil {
		return err
	}

	selections, err := readRMSupportSelections(reader, stdout, entries)
	if err != nil {
		return err
	}

	err = writeRMSupportConfirmation(stdout, selections)
	if err != nil {
		return err
	}

	confirmed, err := readRMSupportConfirmation(reader, stdout)
	if err != nil {
		return err
	}

	if !confirmed {
		return writeRMSupportCancelled(stdout)
	}

	return c.runConfirmedRemoval(ctx, service, stdout, plan, selections)
}

func readRMSupportSelections(
	reader *bufio.Reader,
	stdout io.Writer,
	entries []domain.BIEntry,
) ([]rmSupportSelection, error) {
	_, err := fmt.Fprint(stdout, "select numbers: ")
	if err != nil {
		return nil, fmt.Errorf("write selection prompt: %w", err)
	}

	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read selection input: %w", err)
	}

	numbers, err := parseSelectionNumbers(line, len(entries))
	if err != nil {
		return nil, fmt.Errorf("parse selection input: %w", err)
	}

	return buildSelections(entries, numbers), nil
}

func readRMSupportConfirmation(reader *bufio.Reader, stdout io.Writer) (bool, error) {
	_, err := fmt.Fprint(stdout, "confirm removal targets (yes/no): ")
	if err != nil {
		return false, fmt.Errorf("write confirmation prompt: %w", err)
	}

	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("read confirmation input: %w", err)
	}

	confirmed, parseErr := parseConfirmation(line)
	if parseErr != nil {
		return false, fmt.Errorf("parse confirmation input: %w", parseErr)
	}

	return confirmed, nil
}

func selectedRMSupportKeys(selections []rmSupportSelection) []string {
	keys := make([]string, 0, len(selections))
	for _, selection := range selections {
		keys = append(keys, selection.entry.IDX().Escaped())
	}

	return keys
}

func (c *rmSupportCommand) runConfirmedRemoval(
	ctx context.Context,
	service *flow.Service,
	stdout io.Writer,
	plan *flow.RMSupportPlan,
	selections []rmSupportSelection,
) error {
	err := writeRMSupportIDXList(stdout, selections)
	if err != nil {
		return err
	}

	if c.ShowOmap {
		err = writeRMSupportOmapKeys(
			stdout,
			"before removal",
			plan.IndexPool(),
			plan.Marker(),
			plan.ShardID(),
			plan.OmapKeys(),
		)
		if err != nil {
			return err
		}
	}

	result, err := service.RemoveRMSupportOmapKeys(
		ctx,
		c.ContainerName,
		plan.IndexPool(),
		plan.Marker(),
		plan.ShardID(),
		selectedRMSupportKeys(selections),
	)
	if err != nil {
		return fmt.Errorf("execute rm-support removal: %w", err)
	}

	if !c.ShowOmap {
		return nil
	}

	return writeRMSupportOmapKeys(
		stdout,
		"after removal",
		plan.IndexPool(),
		plan.Marker(),
		plan.ShardID(),
		result.OmapKeys(),
	)
}
