package cli

type zoneCommand struct {
	Default zoneDefaultCommand `cmd:"" help:"Read the default zone from RGW." name:"default"`
}
