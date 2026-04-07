package cli

type bucketCommand struct {
	List   bucketListCommand   `cmd:"" help:"List buckets from RGW."                 name:"list"`
	Index  bucketIndexCommand  `cmd:"" help:"List bucket index entries for a shard." name:"index"`
	Layout bucketLayoutCommand `cmd:"" help:"Read bucket layout from RGW."           name:"layout"`
	Stats  bucketStatsCommand  `cmd:"" help:"Read bucket stats from RGW."            name:"stats"`
}
