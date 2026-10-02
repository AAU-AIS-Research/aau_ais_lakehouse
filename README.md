# AAU AIS Lakehouse

Tools for ingesting and analyzing **Automatic Identification System (AIS)**
maritime movement data as a DuckLake/DuckDB lakehouse.

Provided by Aalborg University, Department of Computer Science.

## Layout

A `uv`-managed monorepo of four packages (see each package's `README.md`):

| Package | Purpose |
|---|---|
| `aau-ais-core` | Shared utilities, settings, and DuckDB helpers |
| `aau-ais-schema` | Dimension/merge load pipeline and SQL migrations |
| `aau-ais-traj` | Trajectory and stop fact loading |
| `aau-ais-cli` | The `aauais` command-line interface and dev stack |

The dependency chain is `core → schema → traj → cli`, wired through editable
`[tool.uv.sources]` references.

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- [Docker](https://www.docker.com/products/docker-desktop/) (for the dev stack)

## Quick start

1. Create a `.env` (e.g. `packages/cli/.env`):

   ```bash
   GIZMOSQL__USER='gizmosql_user'
   GIZMOSQL__PASSWORD='your-password'
   GIZMOSQL__HOST=localhost
   GIZMOSQL__PORT=31337
   ```

2. Start the development GizmoSQL server and create the schema:

   ```bash
   uv run aauais dev start
   ```

3. Load AIS trajectory data from one or more parquet files:

   ```bash
   uv run aauais traj load /path/to/data/*.pq
   ```

4. Stop the stack when finished (data is kept in the `gizmosql_data` volume):

   ```bash
   uv run aauais dev stop
   ```

## CLI

| Command | Description |
|---|---|
| `aauais dev start` / `stop` | Start/stop the Docker dev stack |
| `aauais db create` / `drop` / `compress` | Apply, drop, or compact the schema |
| `aauais traj load <files...>` | Load trajectory files into the lakehouse |

Run `aauais --help` for the full command set.
