package cmd

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"github.com/urfave/cli/v2"
	"k8s.io/klog"

	"github.com/minio/minio-go/v6"
)

// Backupper is our exporter type
type Backupper struct {
	filename    string
	backuppath  string
	concurrency int
	repeat      int
}

// Describe all the metrics we export
func (sb Backupper) Describe(ch chan<- *prometheus.Desc) {
	ch <- s3Success
	ch <- s3Duration
}

/*
// Collect metrics
func (e Backupper) Collect(ch chan<- prometheus.Metric) {
	minioClient, err := minio.New(e.filename, true)
	if err != nil {
		klog.Fatalf("Could not create minioClient to endpoint %s, %v\n", e.endpoint, err)
		return
	}

}

func probeHandler(w http.ResponseWriter, r *http.Request, sb Backupper) {
	registry := prometheus.NewRegistry()
	registry.Register(b)

	// Serve
	h := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	h.ServeHTTP(w, r)
}
*/

func (sb Backupper) backupBucket(buckets <-chan bucket) {
	for {
		select {
		case b := <-buckets:

			var newBucket bucket

			//klog.Infof("process bucket %s", b.Name)
			bucketpath := sb.backuppath + "/" + b.User.Accesskey + "/" + b.Name
			if !fileExists(bucketpath) {
				// new bucket
				os.Mkdir(bucketpath, 0700)
			}
			newBucket.Name = b.Name
			newBucket.User = b.User
			mc, err := minio.New(b.User.Endpoint, b.User.Accesskey, b.User.Secretkey, true)
			newBucket.BucketPolicy, err = mc.GetBucketPolicy(b.Name)
			if err != nil {
				klog.Error(err)
			}
			objects := readBucketJSON(bucketpath+"/bucket.json", &b)

			// loop over all remote objects, refresh object meta data
			doneCh := make(chan struct{})
			for o := range mc.ListObjects(b.Name, "", true, doneCh) {
				//encode object.Key as base64
				oe := base64.StdEncoding.EncodeToString([]byte(o.Key))
				objectPath := bucketpath + "/" + oe
				//klog.Infof("pocess object: %s (%s)", o.Key, objectPath)

				// check if object is missing locally or has changed. In either case, download object
				lo, ok := objects[o.Key]
				if !ok || !fileExists(objectPath) || o.LastModified != lo.LastModified || o.Size != lo.Size {
					klog.Infof("object changed: %s", o.Key)
					mc.FGetObject(b.Name, o.Key, objectPath, minio.GetObjectOptions{})
				}
				newBucket.Objects = append(newBucket.Objects, o)
			}
			close(doneCh)
			// make map of new objects
			newObjects := make(map[string]minio.ObjectInfo)
			for _, o := range newBucket.Objects {
				newObjects[o.Key] = o
			}
			// check for delete objects
			for _, o := range objects {
				if _, ok := newObjects[o.Key]; !ok {
					// object has been deleted, so delete local copy
					os.Remove(bucketpath + "/" + base64.StdEncoding.EncodeToString([]byte(o.Key)))
					klog.Infof("deleting local object: %s (%s)", o.Key, bucketpath+"/"+base64.StdEncoding.EncodeToString([]byte(o.Key)))
				}
			}
			writeBucketJSON(bucketpath+"/bucket.json", newBucket)

		default:
			return
		}
	}
}

func (sb Backupper) backupUser(u user, buckets chan bucket) {
	mc, err := minio.New(u.Endpoint, u.Accesskey, u.Secretkey, true)
	if err != nil {
		klog.Errorf(err.Error())
	}
	userpath := sb.backuppath + "/" + u.Accesskey
	if _, err := os.Stat(userpath); err != nil {
		os.Mkdir(userpath, 0700)
	}
	bl, err := mc.ListBuckets()
	if err != nil {
		klog.Errorf(err.Error())
	}

	// loop over all remote buckts

	for _, b := range bl {
		buckets <- bucket{
			Name: b.Name,
			User: u,
		}
	}
}

func (sb Backupper) backup() error {
	userList, err := readUserFile(sb.filename)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	done := make(chan struct{})
	defer close(done)

	buckets := make(chan bucket, sb.concurrency)
	for _, u := range userList.User {
		go func(u user) {
			sb.backupUser(u, buckets)

		}(u)
	}
	wg.Add(sb.concurrency)
	for i := 0; i < sb.concurrency; i++ {
		go func() {
			sb.backupBucket(buckets)
			wg.Done()
		}()
	}
	wg.Wait()
	close(buckets)
	return nil
}

func startBackup(c *cli.Context) error {

	listenAddress := c.String(flagListenAddress)
	filename := c.String(flagFilename)
	backuppath := c.String(flagBackupPath)
	concurrency := c.Int(flagConcurrency)
	repeat := c.Int(flagRepeat)

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

	b := Backupper{
		filename:    filename,
		backuppath:  backuppath,
		concurrency: concurrency,
		repeat:      repeat,
	}

	log.Infoln("Starting s3 backup", version.Info())
	log.Infoln("Build context", version.BuildContext())

	ticker := time.NewTicker(time.Duration(b.repeat) * time.Minute)
	go func() {
		for ; true; <-ticker.C {
			klog.Info("Starting backup")
			err := b.backup()
			if err != nil {
				klog.Errorf("Backup failed: %s", err)
			}
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
						 <head><title>S3 Backup Client</title></head>
						 <body>
						 <h1>S3 Backup Client</h1>
						 <p><a href='/metrics'>Metrics</a></p>
						 </body>
						 </html>`))
	})

	log.Infoln("Listening on", listenAddress)
	log.Fatal(http.ListenAndServe(listenAddress, nil))

	return nil
}

// BackupCmd starts backup daemon
func BackupCmd() *cli.Command {
	return &cli.Command{
		Name: "backup",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    flagListenAddress,
				Usage:   "Optional. Specify listen address.",
				EnvVars: []string{envListenAddress},
				Value:   defaultListenAddress,
			},
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
				Usage:   "Optional. Specify number of concurrent backup runners. (default: " + string(defaultConcurrency) + ")",
				EnvVars: []string{envConcurrency},
				Value:   defaultConcurrency,
			},
			&cli.IntFlag{
				Name:    flagRepeat,
				Usage:   "Optional. Specify time between backups in minutes. (default: " + string(defaultRepeat) + ")",
				EnvVars: []string{envRepeat},
				Value:   defaultRepeat,
			},
		},
		Action: func(c *cli.Context) error {
			if err := startBackup(c); err != nil {
				klog.Fatalf("Error starting daemon: %v", err)
				return err
			}
			return nil
		},
	}
}
