package main

import (
	"fmt"
	"os"

	appcli "github.com/neatflowcv/cephclient/internal/app/cli"
)

func main() {
	err := appcli.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
