package cli

type app struct {
	BIList       biListCommand       `cmd:"" help:"List bucket index entries for an object shard."   name:"bi-list"`
	BucketLayout bucketLayoutCommand `cmd:"" help:"Read bucket layout from a running RGW container." name:"bucket-layout"`
	BucketStats  bucketStatsCommand  `cmd:"" help:"Read bucket stats from a running RGW container."  name:"bucket-stats"`
	ListBuckets  listBucketsCommand  `cmd:"" help:"List buckets from a running RGW container."       name:"list-buckets"`
	ObjectShard  objectShardCommand  `cmd:"" help:"Read the shard number for an object from RGW."    name:"object-shard"`
}

func newApp() *app {
	return &app{
		BIList: biListCommand{
			ContainerName: "",
			BucketName:    "",
			ObjectName:    "",
			ShardID:       0,
		},
		BucketLayout: bucketLayoutCommand{
			ContainerName: "",
			BucketName:    "",
		},
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
