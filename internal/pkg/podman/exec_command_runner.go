package podman

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

var _ Runner = (*execCommandRunner)(nil)

type execCommandRunner struct {
	podmanPath string
}

func newExecCommandRunner() (*execCommandRunner, error) {
	podmanPath, err := exec.LookPath("podman")
	if err != nil {
		return nil, fmt.Errorf("find podman binary: %w", err)
	}

	return &execCommandRunner{podmanPath: podmanPath}, nil
}

func (r execCommandRunner) Run(ctx context.Context, args ...string) ([]byte, string, error) {
	//nolint:gosec
	cmd := exec.CommandContext(ctx, r.podmanPath, args...)

	var stderr bytes.Buffer

	cmd.Stderr = &stderr

	stdout, err := cmd.Output()

	return stdout, stderr.String(), err
}
