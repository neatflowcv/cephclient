package cli

type app struct {
	Bucket      bucketCommand      `cmd:"" help:"Read bucket data from RGW."                      name:"bucket"`
	Omap        omapCommand        `cmd:"" help:"Inspect OMAP data from an index object."         name:"omap"`
	ObjectShard objectShardCommand `cmd:"" help:"Read an object's shard number from RGW."         name:"object-shard"`
	RMSupport   rmSupportCommand   `cmd:"" help:"Interactively select BI idx values for removal." name:"rm-support"`
	Zone        zoneCommand        `cmd:"" help:"Read zone data from RGW."                        name:"zone"`
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
		Omap: omapCommand{
			List: omapListCommand{
				ContainerName: "",
				IndexPool:     "",
				Marker:        "",
				Shard:         0,
			},
			Rm: omapRmCommand{
				ContainerName: "",
				IndexPool:     "",
				Marker:        "",
				Shard:         0,
				Key:           "",
			},
		},
		ObjectShard: objectShardCommand{
			ContainerName: "",
			ObjectName:    "",
			Shards:        0,
		},
		RMSupport: rmSupportCommand{
			ContainerName: "",
			BucketName:    "",
			ObjectName:    "",
			ShowOmap:      false,
		},
		Zone: zoneCommand{
			Default: zoneDefaultCommand{
				ContainerName: "",
			},
		},
	}
}
