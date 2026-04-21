package domain_test

import (
	"testing"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestBucketIndexObjectRawReturnsRGWBucketIndexObjectName(t *testing.T) {
	t.Parallel()

	object := domain.NewBucketIndexObject("bucket-marker", 0, 7)

	require.Equal(t, "bucket-marker", object.Marker())
	require.Equal(t, 0, object.Layout())
	require.Equal(t, 7, object.Shard())
	require.Equal(t, ".dir.bucket-marker.7", object.Raw())
}

func TestBucketIndexObjectRawIncludesLayoutWhenPositive(t *testing.T) {
	t.Parallel()

	object := domain.NewBucketIndexObject("bucket-marker", 2, 7)

	require.Equal(t, "bucket-marker", object.Marker())
	require.Equal(t, 2, object.Layout())
	require.Equal(t, 7, object.Shard())
	require.Equal(t, ".dir.bucket-marker.2.7", object.Raw())
}
