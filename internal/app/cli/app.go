package cli

type app struct {
	Bucket    bucketCommand    `cmd:"" help:"Read bucket data from RGW."                      name:"bucket"`
	Omap      omapCommand      `cmd:"" help:"Inspect OMAP data from an index object."         name:"omap"`
	Object    objectCommand    `cmd:"" help:"Read object data from RGW."                      name:"object"`
	RMSupport rmSupportCommand `cmd:"" help:"Interactively select BI idx values for removal." name:"rm-support"`
	Zone      zoneCommand      `cmd:"" help:"Read zone data from RGW."                        name:"zone"`
}

func newApp() *app {
	return &app{
		Bucket: bucketCommand{
			List: bucketListCommand{
				Container: "",
			},
			Index: bucketIndexCommand{
				Container:  "",
				Bucket:     "",
				Object:     "",
				Shard:      0,
			},
			Layout: bucketLayoutCommand{
				Container: "",
				Bucket:    "",
			},
			Stats: bucketStatsCommand{
				Container: "",
				Bucket:    "",
			},
		},
		Omap: omapCommand{
			List: omapListCommand{
				Container: "",
				IndexPool: "",
				Marker:    "",
				Shard:     0,
			},
			Rm: omapRmCommand{
				Container: "",
				IndexPool: "",
				Marker:    "",
				Shard:     0,
				Key:       "",
			},
		},
		Object: objectCommand{
			Shard: objectShardCommand{
				Container:   "",
				Object:      "",
				TotalShards: 0,
			},
		},
		RMSupport: rmSupportCommand{
			Container:  "",
			Bucket:     "",
			Object:     "",
			ShowOmap:   false,
		},
		Zone: zoneCommand{
			Default: zoneDefaultCommand{
				Container: "",
			},
		},
	}
}
