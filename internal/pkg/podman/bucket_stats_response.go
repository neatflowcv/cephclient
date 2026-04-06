package podman

type bucketStatsResponse struct {
	ID         string                   `json:"id"`
	Name       string                   `json:"bucket"`
	NumShards  int                      `json:"num_shards"`
	Marker     string                   `json:"marker"`
	Usage      bucketStatsUsageResponse `json:"usage"`
	Versioning string                   `json:"versioning"`
}

type bucketStatsUsageResponse struct {
	RGWMain bucketStatsRGWMainUsageResponse `json:"rgw.main"`
}

type bucketStatsRGWMainUsageResponse struct {
	Size        int64 `json:"size"`
	ObjectCount int   `json:"num_objects"`
}
