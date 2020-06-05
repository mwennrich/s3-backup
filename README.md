# s3-backup

backup/restore a remote s3 store to/from local filesystem

## Example

```bash
./s3-backup backup -f users.json -p ~/tmp/s3backup/
./s3-backup restore -f users.json -p ~/tmp/s3backup/
```

## Usage

```text
USAGE:
   s3-backup backup [command options] [arguments...]

OPTIONS:
   -l value  Optional. Specify listen address for prometheus /metrics. (default: ":2112") [$LISTEN_ADDRESS]
   -f value  Required. Specify filename. [$FILENAME]
   -p value  Required. Specify backuppath. [$BACKUPPATH]
   -c value  Optional. Specify number of concurrent backup runners. (default: 10) [$CONCURRENCY]
   -i value      Optional. Specify time between backups in minutes. (default: 60) [$INTERVAL]
   --key value   Optional. Specify key for TLS. [$KEY]
   --cert value  Optional. Specify cart for TLS. [$CERT]
   --help, -h    show help (default: false)
```

## config file

the config file is a json file with following format:

```json
{
  "users": [
    {
      "displayname": "user1",
      "accesskey": "acceskey1",
      "endpoint": "s3.example.com",
      "secretkey": "secretkey1"
    },
    {
      "displayname": "user2",
      "accesskey": "accesskey2",
      "endpoint": "s3.example.com",
      "secretkey": "secretkey2"
    }
  ]
}
```
