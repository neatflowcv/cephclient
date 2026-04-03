package podman

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

var _ Runner = (*execCommandRunner)(nil)

type execCommandRunner struct {
	podmanPath string
	debug      bool
	stderr     io.Writer
}

func newExecCommandRunner(debug bool, stderr io.Writer) (*execCommandRunner, error) {
	podmanPath, err := exec.LookPath("podman")
	if err != nil {
		return nil, fmt.Errorf("find podman binary: %w", err)
	}

	if stderr == nil {
		stderr = io.Discard
	}

	return &execCommandRunner{
		podmanPath: podmanPath,
		debug:      debug,
		stderr:     stderr,
	}, nil
}

func (r execCommandRunner) Run(ctx context.Context, args ...string) ([]byte, string, error) {
	if r.debug {
		writeDebugCommand(r.stderr, r.podmanPath, args)
	}

	//nolint:gosec
	cmd := exec.CommandContext(ctx, r.podmanPath, args...)

	var stderr bytes.Buffer

	cmd.Stderr = &stderr

	stdout, err := cmd.Output()

	if r.debug {
		writeDebugOutput(r.stderr, "stdout", stdout)
		writeDebugOutput(r.stderr, "stderr", stderr.Bytes())
	}

	return stdout, stderr.String(), err
}

func writeDebugCommand(stderr io.Writer, podmanPath string, args []string) {
	commandArgs := make([]string, 0, len(args)+1)
	commandArgs = append(commandArgs, filepath.Base(podmanPath))

	for _, arg := range args {
		commandArgs = append(commandArgs, quoteDebugArg(arg))
	}

	_, _ = fmt.Fprintf(stderr, "debug: %s\n", strings.Join(commandArgs, " "))
}

func quoteDebugArg(arg string) string {
	if arg == "" || strings.ContainsAny(arg, " \t\n\"'\\") {
		return strconv.Quote(arg)
	}

	return arg
}

func writeDebugOutput(stderr io.Writer, name string, output []byte) {
	_, _ = fmt.Fprintf(stderr, "debug: %s:\n", name)
	_, _ = stderr.Write(output)

	if len(output) == 0 || output[len(output)-1] != '\n' {
		_, _ = io.WriteString(stderr, "\n")
	}
}
