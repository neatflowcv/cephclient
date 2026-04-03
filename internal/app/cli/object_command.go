package cli

type objectCommand struct {
	Shard objectShardCommand `cmd:"" help:"Read an object's shard number from RGW." name:"shard"`
}
