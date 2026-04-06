package cli

type app struct {
	Debug     bool             `help:"Print debug output."`
	Bucket    bucketCommand    `cmd:""                     help:"Read RGW bucket data."           name:"bucket"`
	Omap      omapCommand      `cmd:""                     help:"Inspect index OMAP data."        name:"omap"`
	Object    objectCommand    `cmd:""                     help:"Read RGW object data."           name:"object"`
	Raw       rawCommand       `cmd:""                     help:"Inspect raw Ceph object data."   name:"raw"`
	RMSupport rmSupportCommand `cmd:""                     help:"Select BI idx values to remove." name:"rm-support"`
	Zone      zoneCommand      `cmd:""                     help:"Read RGW zone data."             name:"zone"`
}

func newApp() *app {
	return &app{
		Debug:     false,
		Bucket:    newBucketCommand(),
		Omap:      newOmapCommand(),
		Object:    newObjectCommand(),
		Raw:       newRawCommand(),
		RMSupport: newRMSupportCommand(),
		Zone:      newZoneCommand(),
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
		Inspect: objectInspectCommand{
			Container: "",
			Bucket:    "",
			Object:    "",
		},
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

func newRawCommand() rawCommand {
	return rawCommand{
		Exists: rawExistsCommand{
			Container: "",
			Pool:      "",
			Object:    "",
		},
	}
}

func newRMSupportCommand() rmSupportCommand {
	return rmSupportCommand{
		Container: "",
		Bucket:    "",
		Object:    "",
		ShowOmap:  false,
	}
}

func newZoneCommand() zoneCommand {
	return zoneCommand{
		Default: zoneDefaultCommand{
			Container: "",
		},
	}
}
