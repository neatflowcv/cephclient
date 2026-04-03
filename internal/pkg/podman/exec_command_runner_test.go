//nolint:testpackage
package podman

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecCommandRunnerRunWithoutDebugSkipsDebugOutput(t *testing.T) {
	t.Parallel()

	scriptPath := writeExecutableTestScript(t, "#!/bin/sh\nprintf 'bucket-a\\n'\nprintf 'warn\\n' >&2\n")

	var debugOutput bytes.Buffer

	runner := execCommandRunner{
		podmanPath: scriptPath,
		debug:      false,
		stderr:     &debugOutput,
	}

	stdout, stderr, err := runner.Run(t.Context(), "exec", "-i", "rgw")

	require.NoError(t, err)
	require.Equal(t, "bucket-a\n", string(stdout))
	require.Equal(t, "warn\n", stderr)
	require.Empty(t, debugOutput.String())
}

func TestExecCommandRunnerRunWithDebugPrintsCommandAndOutputs(t *testing.T) {
	t.Parallel()

	scriptPath := writeExecutableTestScript(t, "#!/bin/sh\nprintf 'bucket-a\\n'\nprintf 'warn\\n' >&2\n")

	var debugOutput bytes.Buffer

	runner := execCommandRunner{
		podmanPath: scriptPath,
		debug:      true,
		stderr:     &debugOutput,
	}

	stdout, stderr, err := runner.Run(t.Context(), "exec", "-i", "rgw", "radosgw-admin", "bucket", "list")

	require.NoError(t, err)
	require.Equal(t, "bucket-a\n", string(stdout))
	require.Equal(t, "warn\n", stderr)
	require.Equal(
		t,
		"debug: "+filepath.Base(scriptPath)+" exec -i rgw radosgw-admin bucket list\n"+
			"debug: stdout:\n"+
			"bucket-a\n"+
			"debug: stderr:\n"+
			"warn\n",
		debugOutput.String(),
	)
}

func TestExecCommandRunnerRunWithDebugPrintsCapturedOutputOnFailure(t *testing.T) {
	t.Parallel()

	scriptPath := writeExecutableTestScript(t, "#!/bin/sh\nprintf 'bucket-a'\nprintf 'warn' >&2\nexit 125\n")

	var debugOutput bytes.Buffer

	runner := execCommandRunner{
		podmanPath: scriptPath,
		debug:      true,
		stderr:     &debugOutput,
	}

	stdout, stderr, err := runner.Run(t.Context(), "exec", "-i", "rgw", "sh", "-c", "printf hello world")

	require.Error(t, err)

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, "bucket-a", string(stdout))
	require.Equal(t, "warn", stderr)
	require.Equal(
		t,
		"debug: "+filepath.Base(scriptPath)+" exec -i rgw sh -c \"printf hello world\"\n"+
			"debug: stdout:\n"+
			"bucket-a\n"+
			"debug: stderr:\n"+
			"warn\n",
		debugOutput.String(),
	)
}

func writeExecutableTestScript(t *testing.T, contents string) string {
	t.Helper()

	scriptPath := filepath.Join(t.TempDir(), "fake-podman.sh")
	//nolint:gosec
	err := os.WriteFile(scriptPath, []byte(contents), 0o755)
	require.NoError(t, err)

	return scriptPath
}
