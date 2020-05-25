package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io/ioutil"
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
	totalObjects = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "total_objects",
		Help: "total number of objects in backup",
	},
		[]string{"user", "bucket"},
	)
	changedObjects = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "changed_objects",
		Help: "total number of changed and downloaded objects in backup",
	},
		[]string{"user", "bucket"},
	)
	totalBuckets = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "total_buckets",
		Help: "total number of objects in backup",
	},
		[]string{"user"},
	)
	totalErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "total_errors",
		Help: "total number of errors during backup",
	},
		[]string{"user", "bucket"},
	)
)

// Backupper is our exporter type
type Backupper struct {
	filename    string
	backuppath  string
	concurrency int
	interval    int
}

func (sb *Backupper) logErr(user string, bucket string, err error) {
	if err != nil {
		klog.Error(err)
		totalErrors.With(prometheus.Labels{"bucket": bucket, "user": user}).Inc()
	}
}
func (sb *Backupper) backupBucket(buckets <-chan bucket) {
	for {
		select {
		case b := <-buckets:

			var newBucket bucket
			var objects map[string]minio.ObjectInfo

			//klog.Infof("process bucket %s", b.Name)
			bucketpath := sb.backuppath + "/" + b.User.Accesskey + "/" + b.Name
			if !fileExists(bucketpath) {
				// new bucket
				os.Mkdir(bucketpath, 0700)
				os.Mkdir(bucketpath+"/objects", 0700)
			} else {
				objects = readBucketJSON(bucketpath+"/bucket.json", &b)
			}

			newBucket.Name = b.Name
			newBucket.User = b.User
			mc, err := minio.New(b.User.Endpoint, b.User.Accesskey, b.User.Secretkey, true)
			newBucket.BucketPolicy, err = mc.GetBucketPolicy(b.Name)
			sb.logErr(b.User.Accesskey, b.Name, err)

			// loop over all remote objects, refresh object meta data
			doneCh := make(chan struct{})
			for o := range mc.ListObjects(b.Name, "", true, doneCh) {
				//encode object.Key as base64
				oe := base64.StdEncoding.EncodeToString([]byte(o.Key))
				objectPath := bucketpath + "/objects/" + oe
				//klog.Infof("pocess object: %s (%s)", o.Key, objectPath)

				// check if object is missing locally or has changed. In either case, download object
				lo, ok := objects[o.Key]
				if !ok || !fileExists(objectPath) || o.LastModified != lo.LastModified || o.Size != lo.Size {
					klog.Infof("object changed: %s/%s/%s", b.User.Accesskey, b.Name, o.Key)
					mc.FGetObject(b.Name, o.Key, objectPath, minio.GetObjectOptions{})
					changedObjects.With(prometheus.Labels{"bucket": b.Name, "user": b.User.Accesskey}).Inc()
				}
				newBucket.Objects = append(newBucket.Objects, o)
			}
			close(doneCh)
			// make map of new objects
			newObjects := make(map[string]minio.ObjectInfo)
			totalObjects.With(prometheus.Labels{"bucket": b.Name, "user": b.User.Accesskey}).Set(0)
			for _, o := range newBucket.Objects {
				newObjects[o.Key] = o
				totalObjects.With(prometheus.Labels{"bucket": b.Name, "user": b.User.Accesskey}).Inc()
			}
			// check for delete objects
			for _, o := range objects {
				if _, ok := newObjects[o.Key]; !ok {
					// object has been deleted, so delete local copy
					klog.Infof("deleting local object: %s (%s)", o.Key, bucketpath+"/objects/"+base64.StdEncoding.EncodeToString([]byte(o.Key)))
					os.Remove(bucketpath + "/objects/" + base64.StdEncoding.EncodeToString([]byte(o.Key)))
				}
			}
			writeBucketJSON(bucketpath+"/bucket.json", newBucket)

		default:
			return
		}
	}
}

func (sb *Backupper) backupUser(u user, buckets chan bucket) {
	klog.Infof("Processing user %s", u.Accesskey)
	mc, err := minio.New(u.Endpoint, u.Accesskey, u.Secretkey, true)
	sb.logErr(u.Accesskey, "", err)
	userpath := sb.backuppath + "/" + u.Accesskey
	if _, err := os.Stat(userpath); err != nil {
		os.Mkdir(userpath, 0700)
	}
	bl, err := mc.ListBuckets()
	sb.logErr(u.Accesskey, "", err)
	rb := make(map[string]bool)
	// loop over all remote
	totalBuckets.With(prometheus.Labels{"user": u.Accesskey}).Set(0)
	for _, b := range bl {
		buckets <- bucket{
			Name: b.Name,
			User: u,
		}
		rb[b.Name] = true
		totalBuckets.With(prometheus.Labels{"user": u.Accesskey}).Inc()
	}
	// loop over all backupped buckets
	localbuckets, err := ioutil.ReadDir(userpath)
	if err != nil {
		sb.logErr(u.Accesskey, "", err)
		return
	}
	for _, bp := range localbuckets {
		if !rb[bp.Name()] {
			// bucket has been deleted. so remove local copy
			klog.Infof("bucket %s has been deleted. Removing backup %s", bp.Name(), userpath+"/"+bp.Name())
			os.RemoveAll(userpath + "/" + bp.Name())
		}
	}
}

func (sb *Backupper) backup() {
	userList, err := readUserFile(sb.filename)
	if err != nil {
		sb.logErr("", "", err)
		return
	}

	var wg, wgu sync.WaitGroup
	buckets := make(chan bucket, sb.concurrency)

	done := make(chan struct{})
	defer close(done)

	for _, u := range userList.Users {
		go func(u user) {
			wgu.Add(1)
			sb.backupUser(u, buckets)
			wgu.Done()
		}(u)
	}
	time.Sleep(30 * time.Second)
	wg.Add(sb.concurrency)
	for i := 0; i < sb.concurrency; i++ {
		go func() {
			sb.backupBucket(buckets)
			wg.Done()
		}()
	}
	wgu.Wait()
	wg.Wait()
	close(buckets)
}

func startBackup(c *cli.Context) error {

	listenAddress := c.String(flagListenAddress)
	filename := c.String(flagFilename)
	backuppath := c.String(flagBackupPath)
	concurrency := c.Int(flagConcurrency)
	interval := c.Int(flagInterval)
	key := c.String(flagKey)
	cert := c.String(flagCert)

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
	if cert != "" || key != "" {
		if _, err := os.Stat(backuppath); err != nil {
			return fmt.Errorf("backuppath not found: %s", backuppath)
		}
		if _, err := os.Stat(backuppath); err != nil {
			return fmt.Errorf("backuppath not found: %s", backuppath)
		}

	}

	prometheus.MustRegister(totalBuckets)
	prometheus.MustRegister(totalObjects)
	prometheus.MustRegister(changedObjects)
	prometheus.MustRegister(totalErrors)

	sb := Backupper{
		filename:    filename,
		backuppath:  backuppath,
		concurrency: concurrency,
		interval:    interval,
	}

	log.Infoln("Starting s3 backup", version.Info())
	log.Infoln("Build context", version.BuildContext())

	ticker := time.NewTicker(time.Duration(sb.interval) * time.Minute)

	go func() {
		for ; true; <-ticker.C {
			klog.Info("Starting backup")
			sb.backup()
			klog.Info("Backup done")
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

	if key != "" && cert != "" {
		caCert, err := ioutil.ReadFile(cert)
		if err != nil {
			log.Fatal(err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig := &tls.Config{
			ClientCAs:  caCertPool,
			ClientAuth: tls.RequireAndVerifyClientCert,
		}
		tlsConfig.BuildNameToCertificate()
		server := &http.Server{
			Addr:      listenAddress,
			TLSConfig: tlsConfig,
		}
		log.Infoln("Listening with mTSL on", listenAddress)
		log.Fatal(server.ListenAndServeTLS(cert, key))
	} else {
		log.Infoln("Listening on", listenAddress)
		log.Fatal(http.ListenAndServe(listenAddress, nil))
	}
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
				Usage:   "Optional. Specify number of concurrent backup runners.",
				EnvVars: []string{envConcurrency},
				Value:   defaultConcurrency,
			},
			&cli.IntFlag{
				Name:    flagInterval,
				Usage:   "Optional. Specify time between backups in minutes.",
				EnvVars: []string{envInterval},
				Value:   defaultInterval,
			},
			&cli.StringFlag{
				Name:    flagKey,
				Usage:   "Optional. Specify key for TLS.",
				EnvVars: []string{envKey},
			},
			&cli.StringFlag{
				Name:    flagCert,
				Usage:   "Optional. Specify cart for TLS.",
				EnvVars: []string{envCert},
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
