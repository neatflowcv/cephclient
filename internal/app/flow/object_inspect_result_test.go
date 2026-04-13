package flow_test

import (
	"testing"

	"github.com/neatflowcv/cephclient/internal/app/flow"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestObjectInspectResultExposesRawObjectsWithoutCopy(t *testing.T) {
	t.Parallel()

	rawObjects := []*flow.RawObjectExistence{
		flow.NewRawObjectExistence(domain.NewOLHRawObjectName("bucket-marker", "test.txt"), true),
	}
	result := &flow.ObjectInspectResult{RawObjects: rawObjects}
	replacement := flow.NewRawObjectExistence(
		domain.NewVersionRawObjectName("bucket-marker", "instance-1", "test.txt"),
		false,
	)

	result.RawObjects[0] = replacement

	require.Same(t, replacement, rawObjects[0])
	require.Same(t, replacement, result.RawObjects[0])
	require.False(t, rawObjects[0].Exists())
	require.Equal(t, "version", rawObjects[0].Name().Kind())
}
