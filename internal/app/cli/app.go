package cli

type app struct {
	Bucket       bucketCommand       `cmd:"" help:"Read bucket data from RGW."                      name:"bucket"`
	ListOmapKeys listOmapKeysCommand `cmd:"" help:"List OMAP keys from an index object."            name:"list-omap-keys"`
	ObjectShard  objectShardCommand  `cmd:"" help:"Read an object's shard number from RGW."         name:"object-shard"`
	RmOmapKey    rmOmapKeyCommand    `cmd:"" help:"Remove an OMAP key from an index object."        name:"rm-omap-key"`
	RMSupport    rmSupportCommand    `cmd:"" help:"Interactively select BI idx values for removal." name:"rm-support"`
	ZoneGet      zoneGetCommand      `cmd:"" help:"Read the default zone from RGW."                 name:"zone-get"`
}

func newApp() *app {
	return &app{
		Bucket: bucketCommand{
			List: bucketListCommand{
				ContainerName: "",
			},
			Index: bucketIndexCommand{
				ContainerName: "",
				BucketName:    "",
				ObjectName:    "",
				ShardID:       0,
			},
			Layout: bucketLayoutCommand{
				ContainerName: "",
				BucketName:    "",
			},
			Stats: bucketStatsCommand{
				ContainerName: "",
				BucketName:    "",
			},
		},
		ListOmapKeys: listOmapKeysCommand{
			ContainerName: "",
			IndexPool:     "",
			Marker:        "",
			Shard:         0,
		},
		ObjectShard: objectShardCommand{
			ContainerName: "",
			ObjectName:    "",
			Shards:        0,
		},
		RmOmapKey: rmOmapKeyCommand{
			ContainerName: "",
			IndexPool:     "",
			Marker:        "",
			Shard:         0,
			Key:           "",
		},
		RMSupport: rmSupportCommand{
			ContainerName: "",
			BucketName:    "",
			ObjectName:    "",
			ShowOmap:      false,
		},
		ZoneGet: zoneGetCommand{
			ContainerName: "",
		},
	}
}
