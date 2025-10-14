# Archives downloader

Sync node via archives from S3

## Config example:

```json
{
  "local_ip": "0.0.0.0",
  "port": 30000,
  "storage": {
    "rocksdb_lru_capacity": "2.1 GB",
    "root_dir": "/var/node/db"
  },
  "bucket_name": "archives",
  "s3_provider": {
    "gcs": {
      "credentials_path": "credentials.json"
    }
  }
}

```
