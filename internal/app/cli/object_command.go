package cli

type objectCommand struct {
	Inspect objectInspectCommand `cmd:"" help:"Inspect an RGW object across related lookups." name:"inspect"`
	Rm      objectRmCommand      `cmd:"" help:"Remove a versioned RGW object."                name:"rm"`
	Shard   objectShardCommand   `cmd:"" help:"Read an object's shard number from RGW."       name:"shard"`
}
