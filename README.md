# Tycho Indexers

A set of indexers for the Tycho network built on top of
the [Tycho Light Node](https://github.com/broxus/tycho/tree/master/light-node).

## System Requirements

System requirements depend on the current network load. We recommend using validator hardware requirements:

- 16 cores \ 32 threads CPU
- 128 GB RAM
- 1TB Fast NVMe SSD
- 1 Gbit/s network connectivity
- Public IP address

## Running

To start `tycho-indexer`, run the following command:

```bash
./tycho-indexer \
  --config config.json \
  --global-config global-config.json \
  --logger-config logger.json \
  --import-zerostate zerostate.boc \
  --keys keys.json
```

### Command Line Options

- `--config` — path to the node configuration file (examples can be found in the README of individual indexers)
- `--global-config` — path to the global network configuration (obtained from the network you want to sync with)
- `--import-zerostate` — path to the zerostate file to import (obtained from the network you want to sync with)
- `--keys` — path to the node keys file (automatically generated on first start if it doesn't exist)
- `--logger-config` — path to the logging configuration

## Updating

To update `tycho-indexer` run the following command:

```bash
./bump.sh tycho-commit [optional-tycho-types-commit]
```
