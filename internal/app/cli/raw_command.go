package cli

type rawCommand struct {
	Exists rawExistsCommand `cmd:"" help:"Check whether a raw object exists." name:"exists"`
}
