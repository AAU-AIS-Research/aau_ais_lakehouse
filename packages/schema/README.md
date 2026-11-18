# AAU AIS Schema

## Overview
The `aau-ais-schema` package defines and populates the **star schema** of the
[AAU AIS Lakehouse](../../README.md): a DuckLake/DuckDB lakehouse for
**Automatic Identification System (AIS)** maritime movement data.

It sits in the middle of the monorepo — it depends on the shared utilities and
settings of [`aau-ais-core`](../core/README.md), and the fact loaders of
[`aau-ais-traj`](../traj/README.md) and the [`aauais` CLI](../cli/README.md)
depend on it. The package provides:

- **Schema DDL** — versioned SQL migrations creating the dimension tables,
  fact tables, views and macros.
- **The dimension load pipeline** — an abstract `Dimension` base class, merge
  strategies and pre-processors that map raw AIS attributes onto the star
  schema via ADBC.
- **Load governance** — `LoadContext` tracks every load in `dim.load_dim`
  (row counts, durations, `ingest_per_sec`) and provides an idempotency guard.
- **Ingest utilities** — chunked Arrow ingest (`flight_sql_ingest`) that splits
  batches to respect Flight SQL message-size limits.

## Package Layout
```
src/aau_ais_schema/
├── __init__.py          # exports LoadContext, IngestionContext
├── load_context.py      # transactional load wrapper + idempotency guard
├── exceptions.py        # LoadError
├── utils.py             # flight_sql_ingest, generate_sequence_name
├── dim/
│   ├── __dimension.py   # abstract Dimension base class
│   └── *_dim.py         # one Dimension subclass per dimension + expanders
├── merge_strategies/    # how staged rows are merged into a dimension
└── sql/
    ├── migrations/      # v*.sql create / u*.sql drop (paired, ordered)
    └── compaction.sql   # DuckLake snapshot expiry / file merge / cleanup
```

## The Star Schema
The schema spans two catalogs, both reached through the ADBC SQL/Arrow gateway:

- **`ais`** — the local DuckDB catalog, holding the `dim` schema.
- **`lakehouse`** — the attached DuckLake catalog, holding the `fact` schema
  and serving-layer views in `main`.

### Dimensions (`ais.dim`)
Written by the Python `Dimension` subclasses in `dim/`:

`load_dim`, `date_dim`, `time_dim`, `country_dim`, `vessel_dim`,
`vessel_config_dim`, `transponder_type_dim`, `vessel_type_dim`,
`vessel_name_dim`, `pos_type_dim`, `cargo_type_dim`, `call_sign_dim`,
`destination_dim`, `nav_status_dim`, `traj_type_dim`,
`traj_state_change_dim`, `traj_geom_dim`, `stop_geom_dim`, `gap_type_dim`,
`spatial_part_key_geom_dim`

Surrogate keys are backed by per-table sequences
(e.g. `vessel_dim_vessel_id_seq`); smart keys (e.g. `date_id`, `time_id`) are
carried directly in the source data.

Only present in SQL (no Python loader yet): `depth_grid_dim`,
`half_degree_grid_dim`, `gap_imp_mv_dim`.

### Facts (`lakehouse.fact`)
- `ais_point_fact` — AIS points, partitioned by `spatial_part_key`
  (an int-encoded quadkey prefix, depth default 8 — see
  `aau_ais_core.quadkey_utils`).
- `ais_traj_fact` — trajectory segments ('in motion').
- `ais_stop_fact` — stationary segments.
- `ais_gap_fact` — gaps between consecutive segments of a vessel.

### Views & macros
- `lakehouse.main.vessel_activity` — union of traj/stop facts joined with
  vessel and geometry dimensions.
- `ais.stot(date_key, time_key)` — date/time smart keys to timestamp.
- Quadkey encoders (`quadkey_uint32_encode` etc.), `minutes_since_midnight`.

## Load Pipeline

### `LoadContext` (`load_context.py`)
Transactional wrapper for one source→destination load. On `start()` it
registers a row in `dim.load_dim` (initially `failed=true`); `__exit__` then
calls `stop()` on success or `fail()` (rollback + `failed=true`) on error.
Loaders wrap the fact ingest in
`ctx.ingest_started()` / `ctx.ingest_stopped()` so timing and
`ingest_per_sec` are recorded. `LoadContext.is_loaded(src_id, dst_tbl, con)`
is the idempotency guard — sources already loaded successfully are skipped.

### `Dimension` (`dim/__dimension.py`)
`load(batch, name_map={})` flow:

1. resolve the name map (defaults to identity, overridden per load),
2. run `pre_processors` (e.g. `DateIdExpander`),
3. `__trim` — distinct + rename columns (Jinja SQL over in-memory DuckDB),
4. `__stage` — ingest into a temporary `staging_<table>`,
5. `__merge_strategy(...)` — merge staged rows into the target dimension,
6. return the (possibly extended) batch for the next dimension.

### Merge strategies (`merge_strategies/`)
- `SmartKeyMergeStrategy` — the smart key already exists in the source data;
   inserts only non-existing rows (`insert or ignore`), batch returned as-is.
- `SurrogateKeyInsertStrategy` — inserts without matching, then joins the
  generated surrogate key back onto the batch.
- `SurrogateKeyMergeStrategy` — MERGEs staged rows against existing records
  and returns the batch with surrogate keys attached.

### Caveats
- ADBC drivers ingest geometry columns as BLOB; `Dimension.__cast_geometry_columns`
  can re-cast them via `ST_GeomFromWKB`, but its invocation is currently
  disabled.
- Dimensions live in the local DuckDB catalog while facts live in the DuckLake
  catalog — there are no atomic cross-engine transactions. See
  [packages/traj/README.md](../traj/README.md) for the known duplication
  issues this causes.

## SQL Migrations & Compaction
- `migrations/vNNN_*.sql` — create migrations, applied **ascending**;
- `migrations/uNNN_*.sql` — drop migrations, applied **descending**;
- `compaction.sql` — DuckLake maintenance
  (`ducklake_expire_snapshots`, `ducklake_merge_adjacent_files`,
  `ducklake_cleanup_old_files`).

Current pairs: `v001/u001` initial schema (dims, point/traj/stop facts,
vessel_activity view), `v002/u002` gap tables, `v003/u003` `ais.stot` macro,
`v004/u004` `spatial_part_key_geom_dim`.

Keep `v`/`u` prefixes paired and zero-padded — the CLI selects migrations
purely by name prefix.

## Usage
Requires Python ≥ 3.12. The package declares no console scripts.

```bash
cd packages/schema
uv sync
```

The schema is normally created, dropped and compacted through the CLI package:

```bash
uv run aauais db create    # applies v* migrations ascending
uv run aauais db drop      # applies u* migrations descending
uv run aauais db compress  # runs compaction.sql
```

Or programmatically:

```python
from importlib import resources

from aau_ais_core.settings import HardchargerConnectionSettings

settings = HardchargerConnectionSettings(
    host="localhost", port=31337, user="ais", password="***"
)

with settings.connect(autocommit=True) as con, con.cursor() as cur:
    migrations = resources.files("aau_ais_schema").joinpath("sql", "migrations")
    files = (f for f in migrations.iterdir() if f.name.startswith("v"))
    for migration in sorted(files, key=lambda f: f.name):
        cur.execute(migration.read_text())
```

Loading a dimension from a batch of Arrow data:

```python
from aau_ais_schema.dim import DateDim, DateIdExpander

date_dim = DateDim(con, pre_processors=[DateIdExpander()])
date_dim.load(tbl, name_map={"date_id": "start_date_id"})
date_dim.load(tbl, name_map={"date_id": "end_date_id"})
```

## Contributing
1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Ensure all existing tests pass and add new tests for your changes.
4. Submit a pull request.

## License
This project is licensed under the MIT License.
