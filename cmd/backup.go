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

var (
	flagListenAddress    = "listen"
	envListenAddress     = "LISTEN_ADDRESS"
	defaultListenAddress = ":2112"
	flagFilename         = "filename"
	envFilename          = "FILENAME"
	flagBackupPath       = "backuppath"
	envBackupPath        = "BACKUPPATH"
)

// Backupper is our exporter type
type Backupper struct {
	filename   string
	backuppath string
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

func (sb Backupper) backupUser(users <-chan user) {
	for u := range users {
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
			var bucket, newBucket bucket

			//klog.Infof("process bucket %s", b.Name)
			bucketpath := userpath + "/" + b.Name
			if !fileExists(bucketpath) {
				// new bucket
				os.Mkdir(userpath, 0700)
			}

			newBucket.BucketPolicy, err = mc.GetBucketPolicy(b.Name)
			if err != nil {
				klog.Error(err)
			}
			objects := readBucketJSON(bucketpath+"/bucket.json", &bucket)

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
					klog.Info(o.LastModified, lo.LastModified, o.Size, lo.Size)
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
		}
	}
}

func (sb Backupper) startBackup() error {

	userList, err := readUserFile(sb.filename)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	const numBackupClients = 1

	done := make(chan struct{})
	defer close(done)

	users := make(chan user)

	go func() {
		for _, u := range userList.User {
			//klog.Infof("process user %s", u.Accesskey)
			users <- u
		}
	}()

	wg.Add(numBackupClients)
	for i := 0; i < numBackupClients; i++ {
		go func() {
			sb.backupUser(users)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(users)
	}()
	return nil
}

func startBackup(c *cli.Context) error {

	listenAddress := c.String(flagListenAddress)
	filename := c.String(flagFilename)
	backuppath := c.String(flagBackupPath)
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
		filename:   filename,
		backuppath: backuppath,
	}

	log.Infoln("Starting s3 backup", version.Info())
	log.Infoln("Build context", version.BuildContext())

	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			err := b.startBackup()
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
