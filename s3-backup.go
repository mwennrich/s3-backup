package main

import (
	"os"

	"github.com/mwennrich/s3-backup/cmd"
	"github.com/urfave/cli/v2"
	"k8s.io/klog"
)

var ()

func main() {
	a := cli.NewApp()
	a.Usage = "S3 Backup"
	a.Commands = []*cli.Command{
		cmd.BackupCmd(),
		cmd.RestoreCmd(),
	}

	if err := a.Run(os.Args); err != nil {
		klog.Fatalf("Critical error: %v", err)
	}
}
