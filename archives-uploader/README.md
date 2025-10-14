# Archives uploader

Upload new archives to S3

## Config example:

```json
{
  "local_ip": "0.0.0.0",
  "port": 30000,
  "storage": {
    "rocksdb_lru_capacity": "2.1 GB",
    "root_dir": "/var/node/db"
  },
  "s3_uploader": {
    "bucket_name": "archives",
    "provider": {
      "gcs": {
        "credentials_path": "credentials.json"
      }
    },
    "chunk_size": "10 Mb",
    "max_concurrency": 8,
    "last_archives_to_upload": 5
  }
}
```
