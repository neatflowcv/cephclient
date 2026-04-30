package cli

type objectCommand struct {
	Fix     objectFixCommand     `cmd:"" help:"Fix an RGW object's bucket index."                      name:"fix"`
	Index   objectIndexCommand   `cmd:"" help:"List bucket index entries for a specific object shard." name:"index"`
	Inspect objectInspectCommand `cmd:"" help:"Inspect an RGW object across related lookups."          name:"inspect"`
	Purge   objectPurgeCommand   `cmd:"" help:"Purge all versioned entries for an RGW object."         name:"purge"`
	Rm      objectRmCommand      `cmd:"" help:"Remove a versioned RGW object."                         name:"rm"`
	Shard   objectShardCommand   `cmd:"" help:"Read an object's shard number from RGW."                name:"shard"`
}
