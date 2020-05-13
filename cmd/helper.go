package cmd

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"k8s.io/klog"

	"github.com/minio/minio-go/v6"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	s3Success = prometheus.NewDesc(
		"probe_success",
		"Displays whether or not the probe was a success",
		[]string{"operation", "s3_endpoint"}, nil,
	)
	s3Duration = prometheus.NewDesc(
		"probe_duration_seconds",
		"Returns how long the probe took to complete in seconds",
		[]string{"operation", "s3_endpoint"}, nil,
	)
)

type user struct {
	Accesskey string `json:"accesskey"`
	Secretkey string `json:"secretkey"`
	Endpoint  string `json:"endpoint"`
}

type users struct {
	User []user `json:"user"`
}

type bucket struct {
	BucketPolicy string             `json:"bucketpolicy"`
	Objects      []minio.ObjectInfo `json:"object"`
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}
func readUserFile(filename string) (users, error) {
	var userList users

	f, err := os.Open(filename)
	if err != nil {
		return userList, err
	}

	j, _ := ioutil.ReadAll(f)
	err = json.Unmarshal(j, &userList)
	if err != nil {
		return userList, err
	}
	return userList, nil
}

func readBucketJSON(filename string, bucket *bucket) map[string]minio.ObjectInfo {

	// read bucket.json and make map of known objects
	objects := make(map[string]minio.ObjectInfo)
	if fileExists(filename) {
		f, err := os.Open(filename)
		if err != nil {
			klog.Error(err)
		}
		j, _ := ioutil.ReadAll(f)
		f.Close()
		if err != nil {
			klog.Error(err)
		}
		if err = json.Unmarshal(j, &bucket); err != nil {
			klog.Error(err)
		}
		for _, o := range bucket.Objects {
			objects[o.Key] = o
		}
	}
	return objects
}

func writeBucketJSON(filename string, bucket bucket) {
	j, err := json.Marshal(bucket)
	if err != nil {
		klog.Error(err)
	}
	err = ioutil.WriteFile(filename, j, 0600)
	if err != nil {
		klog.Error(err.Error)
	}
}