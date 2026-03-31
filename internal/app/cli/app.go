package cli

type app struct {
	BIList       biListCommand       `cmd:"" help:"List bucket index entries for an object shard." name:"bi-list"`
	BucketLayout bucketLayoutCommand `cmd:"" help:"Read bucket layout from RGW."                   name:"bucket-layout"`
	BucketStats  bucketStatsCommand  `cmd:"" help:"Read bucket stats from RGW."                    name:"bucket-stats"`
	ListBuckets  listBucketsCommand  `cmd:"" help:"List buckets from RGW."                         name:"list-buckets"`
	ObjectShard  objectShardCommand  `cmd:"" help:"Read an object's shard number from RGW."        name:"object-shard"`
	ZoneGet      zoneGetCommand      `cmd:"" help:"Read the default zone from RGW."                name:"zone-get"`
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
		ZoneGet: zoneGetCommand{
			ContainerName: "",
		},
	}
}
