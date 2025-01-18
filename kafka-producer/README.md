Example config:

```json
{
  "public_ip": "127.0.0.1",
  "local_ip": "0.0.0.0",
  "port": 30000,
  "network": {
    "quic": null,
    "connection_manager_channel_capacity": 128,
    "connectivity_check_interval": "5s",
    "max_frame_size": 104857600,
    "connect_timeout": "10s",
    "connection_backoff": "10s",
    "max_connection_backoff": "1m",
    "connection_error_delay": "3s",
    "max_concurrent_outstanding_connections": 100,
    "max_concurrent_connections": null,
    "active_peers_event_channel_capacity": 128,
    "shutdown_idle_timeout": "1m",
    "enable_0rtt": false
  },
  "dht": {
    "max_k": 6,
    "max_peer_info_ttl": "1h",
    "max_stored_value_ttl": "1h",
    "max_storage_capacity": 10000,
    "storage_item_time_to_idle": null,
    "local_info_refresh_period": "1m",
    "local_info_announce_period": "10m",
    "local_info_announce_period_max_jitter": "1m",
    "routing_table_refresh_period": "10m",
    "routing_table_refresh_period_max_jitter": "1m",
    "announced_peers_channel_capacity": 10
  },
  "peer_resolver": {
    "max_parallel_resolve_requests": 100,
    "min_ttl_sec": 600,
    "update_before_sec": 1200,
    "fast_retry_count": 10,
    "min_retry_interval": "1s",
    "max_retry_interval": "2m",
    "stale_retry_interval": "10m"
  },
  "overlay": {
    "public_overlay_peer_store_period": "3m",
    "public_overlay_peer_store_max_jitter": "30s",
    "public_overlay_peer_store_max_entries": 20,
    "public_overlay_peer_exchange_period": "3m",
    "public_overlay_peer_exchange_max_jitter": "30s",
    "public_overlay_peer_discovery_period": "3m",
    "public_overlay_peer_discovery_max_jitter": "30s",
    "exchange_public_entries_batch": 20
  },
  "public_overlay_client": {
    "neighbours_update_interval": "2m",
    "neighbours_ping_interval": "30s",
    "max_neighbours": 5,
    "max_ping_tasks": 5,
    "default_roundtrip": "300ms"
  },
  "storage": {
    "root_dir": "/var/node/db",
    "rocksdb_enable_metrics": false,
    "rocksdb_lru_capacity": "32 GB",
    "cells_cache_size": "4.3 GB"
  },
  "blockchain_rpc_service": {
    "max_key_blocks_list_len": 8,
    "serve_persistent_states": true
  },
  "blockchain_block_provider": {
    "get_next_block_polling_interval": "1s",
    "get_block_polling_interval": "1s"
  },
  "kafka": {
    "brokers": "broker1:9092,broker2:9092,broker3:9092",
    "topic": "transactions",
    "group_id": "tycho-kafka-producer",
    "security_config": {
      "kind": "Ssl",
      "security_protocol": "Ssl",
      "ssl_ca_location": "/var/node/ssl/ca.crt",
      "ssl_certificate_location": "/var/node/ssl/client.crt",
      "ssl_key_location": "/var/node/ssl/client.key"
    }
  }
}
```