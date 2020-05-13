package cmd

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.com/urfave/cli/v2"
	"k8s.io/klog"

	"github.com/minio/minio-go/v6"
)

// Restorer type
type Restorer struct {
	filename    string
	backuppath  string
	concurrency int
}

func (sr Restorer) restoreUser(users <-chan user, done <-chan bool) {
	select {
	case u := <-users:
		mc, err := minio.New(u.Endpoint, u.Accesskey, u.Secretkey, true)
		if err != nil {
			klog.Errorf(err.Error())
		}
		userpath := sr.backuppath + "/" + u.Accesskey
		if _, err := os.Stat(userpath); err != nil {
			klog.Errorf("backup %s not found: %s", userpath, err)
			return
		}
		bl, err := mc.ListBuckets()
		if err != nil {
			klog.Errorf(err.Error())
			return
		}
		if len(bl) != 0 {
			klog.Errorf("existing buckets for % found. Aborting restore.", u.Accesskey)
			return
		}

		// loop over all backupped buckets
		buckets, err := ioutil.ReadDir(userpath)
		if err != nil {
			klog.Error(err)
			return
		}

		for _, bp := range buckets {
			var bucket bucket

			if !bp.IsDir() {
				klog.Errorf("% is not a directory. Aborting")
				return
			}
			_ = readBucketJSON(userpath+"/"+bp.Name()+"/bucket.json", &bucket)
			if exists, err := mc.BucketExists(bucket.Name); err != nil || exists {
				klog.Errorf("bucket %s already exists. Aborting.%s", bucket.Name, err)
				return
			}
			if err := mc.MakeBucket(bucket.Name, ""); err != nil {
				klog.Errorf("Unable to create bucket %s. %s. Aborting", bucket.Name, err)
				return
			}

			if err = mc.SetBucketPolicy(bucket.Name, bucket.BucketPolicy); err != nil {
				klog.Errorf("Unable to set BucketPolicy for bucket %s. %s. Aborting", bucket.Name, err)
				return
			}
			for _, o := range bucket.Objects {
				//encode object.Key as base64
				oe := base64.StdEncoding.EncodeToString([]byte(o.Key))
				objectPath := userpath + "/" + bp.Name() + "/" + oe
				//klog.Infof("pocess object: %s (%s)", o.Key, objectPath)

				options := minio.PutObjectOptions{
					UserMetadata: o.UserMetadata,
					UserTags:     o.UserTags,
					ContentType:  o.ContentType,
					StorageClass: o.StorageClass,
				}

				if _, err := mc.FPutObject(bucket.Name, o.Key, objectPath, options); err != nil {
					klog.Errorf("Unalble to upload object %s to bucket %s. %s. Aborting", o.Key, bucket.Name, err)
					return
				}
				klog.Infof("successfully restored %s to %s", o.Key, bucket.Name)
			}
		}
		klog.Infof("%s successfully restored", u.Accesskey)
	case <-done:
		return
	}
}

func (sr Restorer) restore() error {
	userList, err := readUserFile(sr.filename)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	const numBackupClients = 10

	done := make(chan bool)
	defer close(done)

	users := make(chan user)

	go func() {
		for _, u := range userList.User {
			//klog.Infof("process user %s", u.Accesskey)
			users <- u
		}
		for i := 0; i < numBackupClients; i++ {
			done <- true
		}
	}()

	wg.Add(numBackupClients)
	for i := 0; i < numBackupClients; i++ {
		klog.Info("start retorer")
		go func() {
			sr.restoreUser(users, done)
			wg.Done()
		}()
	}
	wg.Wait()
	close(users)
	return nil
}

func startRestore(c *cli.Context) error {

	filename := c.String(flagFilename)
	backuppath := c.String(flagBackupPath)
	concurrency := c.Int(flagConcurrency)
	if filename == "" {
		return fmt.Errorf("invalid empty flag %v", flagFilename)
	}
	if _, err := os.Stat(filename); err != nil {
		return fmt.Errorf("file not found: %s", filename)
	}
	if backuppath == "" {
		return fmt.Errorf("invalid empty flag %v", flagBackupPath)
	}
	if _, err := os.Stat(backuppath); err != nil {
		return fmt.Errorf("backuppath not found: %s", backuppath)
	}

	r := Restorer{
		filename:    filename,
		backuppath:  backuppath,
		concurrency: concurrency,
	}
	r.restore()
	return nil
}

// RestoreCmd restores backup to s3
func RestoreCmd() *cli.Command {
	return &cli.Command{
		Name: "restore",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    flagFilename,
				Usage:   "Required. Specify filename.",
				EnvVars: []string{envFilename},
			},
			&cli.StringFlag{
				Name:    flagBackupPath,
				Usage:   "Required. Specify backuppath.",
				EnvVars: []string{envBackupPath},
			},
			&cli.IntFlag{
				Name:    flagConcurrency,
				Usage:   "Optional. Specify number of concurrent restore runners. (default: " + string(defaultConcurrency) + ")",
				EnvVars: []string{envConcurrency},
				Value:   defaultConcurrency,
			},
		},
		Action: func(c *cli.Context) error {
			if err := startRestore(c); err != nil {
				klog.Fatalf("Error starting daemon: %v", err)
				return err
			}
			return nil
		},
	}
}
