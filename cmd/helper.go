package cmd

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"k8s.io/klog"

	"github.com/minio/minio-go/v6"
)

var (
	flagListenAddress    = "l"
	envListenAddress     = "LISTEN_ADDRESS"
	defaultListenAddress = ":2112"
	flagFilename         = "f"
	envFilename          = "FILENAME"
	flagBackupPath       = "p"
	envBackupPath        = "BACKUPPATH"
	flagConcurrency      = "c"
	envConcurrency       = "CONCURRENCY"
	defaultConcurrency   = 10
	flagInterval         = "i"
	envInterval          = "INTERVAL"
	defaultInterval      = 60
	flagKey              = "key"
	envKey               = "KEY"
	flagCert             = "cert"
	envCert              = "CERT"
)

type key struct {
	Accesskey string `json:"access_key"`
	Secretkey string `json:"secret_key"`
}

type user struct {
	Name      string `json:"displayname"`
	Endpoint  string `json:"endpoint"`
	Accesskey string `json:"accesskey"`
	Secretkey string `json:"secretkey"`
	Keys      []key  `json:"keys"`
}

type users struct {
	Users []user `json:"users"`
}

type bucket struct {
	Name         string
	User         user
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
	for i := range userList.Users {
		// Get working keypair from keys lists
		if (userList.Users[i].Accesskey == "" || userList.Users[i].Secretkey == "") && len(userList.Users[i].Keys) > 0 {
			userList.Users[i].Accesskey = userList.Users[i].Keys[0].Accesskey
			userList.Users[i].Secretkey = userList.Users[i].Keys[0].Secretkey
		}

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
	} else {
		klog.Infof("file %s does not exist", filename)
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
