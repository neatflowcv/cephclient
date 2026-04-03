package cli

type omapCommand struct {
	List omapListCommand `cmd:"" help:"List OMAP keys from an index object."     name:"list"`
	Rm   omapRmCommand   `cmd:"" help:"Remove an OMAP key from an index object." name:"rm"`
}
