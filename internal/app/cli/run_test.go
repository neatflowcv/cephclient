package cli_test

import (
	"bytes"
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/cli"
	"github.com/stretchr/testify/require"
)

func TestRunWithArgsReturnsParseErrorForMissingArgs(t *testing.T) {
	t.Parallel()

	// Arrange
	var stdout bytes.Buffer

	// Act
	err := cli.RunWithArgs(t.Context(), []string{"bucket-stats", "rgw"}, &stdout)

	// Assert
	require.Error(t, err)
	require.Contains(t, err.Error(), "<bucket-name>")
}
