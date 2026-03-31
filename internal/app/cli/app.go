package cli

type app struct {
	BucketStats bucketStatsCommand `cmd:"" help:"Read bucket stats from a running RGW container." name:"bucket-stats"`
	ListBuckets listBucketsCommand `cmd:"" help:"List buckets from a running RGW container."      name:"list-buckets"`
	ObjectShard objectShardCommand `cmd:"" help:"Read the shard number for an object from RGW."   name:"object-shard"`
}

func newApp() *app {
	return &app{
		BucketStats: bucketStatsCommand{
			ContainerName: "",
			BucketName:    "",
		},
		ListBuckets: listBucketsCommand{
			ContainerName: "",
		},
		ObjectShard: objectShardCommand{
			ContainerName: "",
			ObjectName:    "",
			Shards:        0,
		},
	}
}
