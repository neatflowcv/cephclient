package podman

import "github.com/neatflowcv/cephclient/internal/pkg/domain"

type bucketLayoutResponse struct {
	Layout bucketLayoutOuter `json:"layout"`
}

type bucketLayoutOuter struct {
	CurrentIndex bucketLayoutCurrentIndex `json:"current_index"`
}

type bucketLayoutCurrentIndex struct {
	Generation int `json:"gen"`
}

func (r bucketLayoutResponse) toDomain() *domain.Layout {
	return domain.NewLayout(r.Layout.CurrentIndex.Generation)
}
