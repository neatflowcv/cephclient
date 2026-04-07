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
	var ret app

	return &ret
}
