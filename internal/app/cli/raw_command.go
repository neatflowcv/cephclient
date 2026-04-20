package cli

type rawCommand struct {
	Exists rawExistsCommand `cmd:"" help:"Check whether a raw object exists." name:"exists"`
	Rm     rawRmCommand     `cmd:"" help:"Remove a raw object."               name:"rm"`
}
