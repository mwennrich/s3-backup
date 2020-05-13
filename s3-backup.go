package main

import (
	"os"

	"github.com/mwennrich/s3-backup/cmd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	"github.com/urfave/cli/v2"
	"k8s.io/klog"
)

var ()

func init() {
	prometheus.MustRegister(version.NewCollector("s3_backup"))
}

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
