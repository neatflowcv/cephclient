package cli

type bucketCommand struct {
	Index  bucketIndexCommand  `cmd:"" help:"List bucket index entries for an object shard." name:"index"`
	Layout bucketLayoutCommand `cmd:"" help:"Read bucket layout from RGW."                   name:"layout"`
	Stats  bucketStatsCommand  `cmd:"" help:"Read bucket stats from RGW."                    name:"stats"`
}
