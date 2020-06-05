package cmd

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

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

func (sr Restorer) restoreBucket(buckets <-chan bucket) {
	for {
		select {
		case bucket := <-buckets:
			mc, err := minio.New(bucket.User.Endpoint, bucket.User.Accesskey, bucket.User.Secretkey, true)
			if err != nil {
				klog.Errorf(err.Error())
			}
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
				objectPath := sr.backuppath + "/" + bucket.User.Endpoint + "/" + base64.StdEncoding.EncodeToString([]byte(bucket.User.Name)) + "/" + bucket.Name + "/objects/" + oe

				options := minio.PutObjectOptions{
					UserMetadata: o.UserMetadata,
					UserTags:     o.UserTags,
					ContentType:  o.ContentType,
					StorageClass: o.StorageClass,
				}
				if _, err := mc.FPutObject(bucket.Name, o.Key, objectPath, options); err != nil {
					klog.Errorf("Unable to upload object %s to bucket %s. %s. Aborting", o.Key, bucket.Name, err)
					return
				}
				klog.Infof("successfully restored %s to %s/%s", o.Key, bucket.User.Name, bucket.Name)
			}
		default:
			return
		}
	}
}
func (sr Restorer) restoreUser(u user, buckets chan bucket) {
	mc, err := minio.New(u.Endpoint, u.Accesskey, u.Secretkey, true)
	if err != nil {
		klog.Errorf("failed to connect to %s with %s", u.Endpoint, u.Accesskey)
	}
	userpath := sr.backuppath + "/" + u.Endpoint + "/" + base64.StdEncoding.EncodeToString([]byte(u.Name))
	if _, err := os.Stat(userpath); err != nil {
		klog.Errorf("backup %s not found: %s", userpath, err)
		return
	}
	bl, err := mc.ListBuckets()

	if err != nil {
		klog.Errorf("failed to list buckets %s", err)
		return
	}
	if len(bl) != 0 {
		klog.Errorf("existing buckets for %s found. Aborting restore.", u.Name)
		return
	}

	// loop over all backupped buckets
	localbuckets, err := ioutil.ReadDir(userpath)
	if err != nil {
		klog.Errorf("unable to read dir %s: %s", userpath, err)
		return
	}
	klog.Infof("Restoring buckets from %s", userpath)
	for _, bp := range localbuckets {
		if !bp.IsDir() {
			klog.Errorf("%s is not a directory. Aborting")
			return
		}
		klog.Infof("Restore bucket %s", userpath+"/"+bp.Name())
		var bucket bucket
		_ = readBucketJSON(userpath+"/"+bp.Name()+"/bucket.json", &bucket)
		bucket.User.Accesskey = u.Keys[0].Accesskey
		bucket.User.Secretkey = u.Keys[0].Secretkey
		buckets <- bucket
	}
}

func (sr Restorer) restore() error {
	userList, err := readUserFile(sr.filename)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup

	buckets := make(chan bucket, sr.concurrency)

	for _, u := range userList.Users {
		go func(u user) {
			klog.Infof("begin restore user %s", u.Name)
			sr.restoreUser(u, buckets)
		}(u)
	}
	time.Sleep(30 * time.Second)
	wg.Add(sr.concurrency)
	for i := 0; i < sr.concurrency; i++ {
		go func(i int) {
			sr.restoreBucket(buckets)
			wg.Done()
		}(i)
	}
	wg.Wait()
	close(buckets)
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
	klog.Info("done")
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
