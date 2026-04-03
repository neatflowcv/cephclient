package cli

type bucketCommand struct {
	Stats bucketStatsCommand `cmd:"" help:"Read bucket stats from RGW." name:"stats"`
}
