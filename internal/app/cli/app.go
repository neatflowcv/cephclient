package cli

type app struct {
	Debug     bool             `help:"Print debug output."`
	Bucket    bucketCommand    `cmd:""                     help:"Read RGW bucket data."           name:"bucket"`
	Omap      omapCommand      `cmd:""                     help:"Inspect index OMAP data."        name:"omap"`
	Object    objectCommand    `cmd:""                     help:"Read RGW object data."           name:"object"`
	RMSupport rmSupportCommand `cmd:""                     help:"Select BI idx values to remove." name:"rm-support"`
	Zone      zoneCommand      `cmd:""                     help:"Read RGW zone data."             name:"zone"`
}

func newApp() *app {
	return &app{
		Debug: false,
		Bucket:    newBucketCommand(),
		Omap:      newOmapCommand(),
		Object:    newObjectCommand(),
		RMSupport: rmSupportCommand{
			Container: "",
			Bucket:    "",
			Object:    "",
			ShowOmap:  false,
		},
		Zone: zoneCommand{
			Default: zoneDefaultCommand{
				Container: "",
			},
		},
	}
}

func newBucketCommand() bucketCommand {
	return bucketCommand{
		List: bucketListCommand{
			Container: "",
		},
		Index: bucketIndexCommand{
			Container: "",
			Bucket:    "",
			Object:    "",
			Shard:     0,
		},
		Layout: bucketLayoutCommand{
			Container: "",
			Bucket:    "",
		},
		Stats: bucketStatsCommand{
			Container: "",
			Bucket:    "",
		},
	}
}

func newOmapCommand() omapCommand {
	return omapCommand{
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
	}
}

func newObjectCommand() objectCommand {
	return objectCommand{
		Rm: objectRmCommand{
			Container: "",
			Bucket:    "",
			Object:    "",
			Version:   "",
		},
		Shard: objectShardCommand{
			Container:   "",
			Object:      "",
			TotalShards: 0,
		},
	}
}
