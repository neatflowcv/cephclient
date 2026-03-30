package cli

type app struct {
	BucketStats bucketStatsCommand `cmd:"" help:"Read bucket stats from a running RGW container." name:"bucket-stats"`
}

func newApp() *app {
	return &app{
		BucketStats: bucketStatsCommand{
			ContainerName: "",
			BucketName:    "",
		},
	}
}
