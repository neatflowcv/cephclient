package cli

type bucketCommand struct {
	Layout bucketLayoutCommand `cmd:"" help:"Read bucket layout from RGW." name:"layout"`
	Stats  bucketStatsCommand  `cmd:"" help:"Read bucket stats from RGW."  name:"stats"`
}
