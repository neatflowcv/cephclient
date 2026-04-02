package podman

type bucketStatsResponse struct {
	ID         string `json:"id"`
	NumShards  int    `json:"num_shards"`
	Versioning string `json:"versioning"`
}
