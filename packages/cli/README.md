# README

## Prerequisites
### Required
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
### Recommended
- [uv package manager](https://docs.astral.sh/uv/getting-started/installation/)


## Usage
First, some environment variables should be set. You can do this by creating a `.env` file in the current working directory (e.g. `./packages/cli/.env`) and adding the following content:
```bash
GIZMOSQL__USER='default_user'
GIZMOSQL__PASSWORD='default_pa$$w0rd'
GIZMOSQL__HOST=localhost
GIZMOSQL__PORT=31337
```
Username and password can be adjusted according to your needs

If using uv you can run the following command to start the gizmosql service: `uv run aauais dev start` followed by one of the following commands
- For loading a directory of parquet files - `uv run aauais load-dir /path/to/data` 
- For loading a single parquet file - `uv run aauais load /path/to/data.pq`

If `uv` is not used, the cli package should be installed where the `aauais` cli should be available.