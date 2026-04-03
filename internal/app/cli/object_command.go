package cli

type objectCommand struct {
	Rm    objectRmCommand    `cmd:"" help:"Remove a versioned RGW object."          name:"rm"`
	Shard objectShardCommand `cmd:"" help:"Read an object's shard number from RGW." name:"shard"`
}
