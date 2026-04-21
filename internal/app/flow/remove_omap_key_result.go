package flow

type RemoveOmapKeyRequest struct {
	ContainerName string
	BucketName    string
	IndexPool     string
	Marker        string
	ShardID       int
	Key           string
}
