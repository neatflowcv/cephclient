package domain_test

import (
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestNewOLHRawObjectName(t *testing.T) {
	t.Parallel()

	name := domain.NewOLHRawObjectName("bucket-marker", "test.txt")

	require.Equal(t, "olh", name.Kind())
	require.Equal(t, "bucket-marker_test.txt", name.Value())
}

func TestNewVersionRawObjectName(t *testing.T) {
	t.Parallel()

	name := domain.NewVersionRawObjectName("bucket-marker", "instance-1", "test.txt")

	require.Equal(t, "version", name.Kind())
	require.Equal(t, "bucket-marker__:instance-1_test.txt", name.Value())
}
