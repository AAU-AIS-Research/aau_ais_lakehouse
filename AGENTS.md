# AGENTS.md

Guidance for AI agents (and humans) working in this repository.

## Project Overview

`aau-ais-lakehouse` is a **`uv`-managed Python monorepo** of tools for handling
**Automatic Identification System (AIS)** maritime movement data, built as a
DuckLake/DuckDB lakehouse. It ingests AIS trajectory parquet files into a
star-schema database (dimensions + facts) through **GizmoSQL**, a SQL/Arrow
gateway over DuckDB and DuckLake.

Provided by Aalborg University, Department of Computer Science.

## Repository Layout

There is **no top-level `pyproject.toml`**. Each package is independent and
self-contained under `packages/`. Cross-package dependencies are wired via
`[tool.uv.sources]` (editable path refs).

```
.
├── README.md
├── LICENSE
├── .gitignore
├── .vscode/
│   └── aau-ais.code-workspace      # ruff format-on-save + organize-imports
└── packages/
    ├── core/    aau-ais-core       # shared utilities & settings (leaf)
    ├── schema/  aau-ais-schema     # dimension/merge/load pipeline + SQL
    ├── traj/    aau-ais-traj       # fact loading (trajectory & stop)
    └── cli/     aau-ais-cli        # Typer CLI + dev Docker stack
```

Each package uses a **src layout**:
`packages/<pkg>/pyproject.toml` + `src/aau_ais_<pkg>/…` (and `tests/` for core).

## Package Dependency Graph

```
aau-ais-core  (no internal deps)
    ▲
    ├── aau-ais-schema   (depends on core)
    │       ▲
    │       └── aau-ais-traj   (depends on schema)
    │           ▲
    │           └── aau-ais-cli   (depends on core + traj)
    └── aau-ais-cli
```

- **core** — leaf; no internal deps.
- **schema** → core.
- **traj** → schema.
- **cli** → core + traj.

All intra-package deps are declared editable in `[tool.uv.sources]`
(e.g. `aau-ais-core = { path = '../core', editable = true }`).

## Python Versions & Build

- **core, schema, traj**: `requires-python = ">=3.12"`
- **cli**: `requires-python = ">=3.13"`
- **Build backend** for every package: `uv_build`
  (`requires = ["uv_build>=0.11.6/0.11.7,<0.12.0"]`).
- **Test runner**: `pytest` (core dev group: `pytest==8.4.*`).
- **Formatting/linting**: `ruff` (configured to run on save via the VS Code
  workspace; also does `organize-imports`).

## Key Third-Party Libraries

| Library | Where / Why |
|---|---|
| `duckdb==1.5.*` | Local engine; `duckdb_utils.get_spatial_con` loads the `spatial` extension for geometry. |
| `adbc-driver-gizmosql==1.1.*` | ADBC driver to connect to GizmoSQL (core, traj). |
| `adbc-driver-manager==1.11.*` | `Connection`/`OperationalError` types (schema, cli). |
| `pyarrow>=22/23` | Arrow tables; `read_parquet`, `adbc_ingest`, `fetch_arrow_table`. |
| `jinja2==3.*` | SQL templating (`JINJA_ENV` in traj, templates in schema). |
| `pydantic` / `pydantic-settings==2.*` | Connection & app settings (core, cli). |
| `typer==0.24.*` | CLI framework (cli). |
| `rich` | Pretty console output (`from rich import print`). |

## Tooling / Commands

This repo is driven by **`uv`**. Run from the repo root (or a package dir).

```bash
# Install/sync a package (and its editable deps)
uv sync                 # inside a package directory
uv run pytest           # run tests (core has tests/)
uv run aauais …         # run the CLI (cli package script: aauais)
```

### Entry points (console scripts)

| Script | Defined in | Target |
|---|---|---|
| `aauais` | cli | `aau_ais_cli.main:app` (the Typer app) |
| `schema` | schema | `schema:main` |
| `lakehouse` | traj | `aau_traj.main:app` ⚠️ **likely broken** (see Caveats) |

### CLI usage (`aauais`)

The CLI is a Typer app (`packages/cli/src/aau_ais_cli/main.py`) with three
sub-commands, all sharing a single cached `Settings` via a `settings_factory`
callback (context `obj`, exposed as `AISContext`):

- `db`
  - `create` — applies `v*` migrations in `aau_ais_schema/sql/migrations/` ascending.
  - `drop` — confirms, then applies `u*` migrations descending.
  - `compress` — runs `sql/compaction.sql` (DuckLake expire/merge/cleanup).
- `dev`
  - `start` — `docker compose -f …/docker/compose.dev.yaml up -d`, waits for
    GizmoSQL readiness, then runs `db.create`. Options: `--env-file`, `--public`
    (binds `0.0.0.0` via `GIZMOSQL_IP`).
  - `stop` — `docker compose … down`.
- `traj`
  - `load <files...>` — validates each file with `utils.is_traj_file`, reads the
    parquet via `get_spatial_con`, and loads it into
    `lakehouse.fact.ais_traj_fact` and `lakehouse.fact.ais_stop_fact`.
  - `load-dir <dir>` — **deprecated** (use `load` with globs/brace expansion).

All DB commands connect through `settings.gizmosql.connect(...)` and execute
migrations/queries via an ADBC `Connection` cursor.

## Architecture: the Load Pipeline

### `LoadContext` (schema)
`aau_ais_schema/load_context.py` is the transactional wrapper for a single
source→destination load. It registers a row in `dim.load_dim` (initially
`failed=true`), and via `__enter__/__exit__` calls `stop()` (mark `failed=false`
on success) or `fail()` (rollback + mark `failed=true` on error).
`LoadContext.is_loaded(src_id, dst_tbl, con)` (static) is the **idempotency
guard** — used by the CLI to skip files already loaded. It tracks timing and
`ingest_per_sec`.

### `Dimension` (schema)
`aau_ais_schema/dim/__dimension.py` defines the abstract `Dimension` base class.
Its `load(batch: pyarrow.Table, name_map={})` flow:
1. resolve name map (`__resolve_name_map`)
2. run `pre_processors`
3. `__trim` — distinct + rename columns (Jinja SQL over an in-memory DuckDB)
4. `__stage` — `utils.flight_sql_ingest` into `staging_<table>` (replace, temp)
5. `__merge_strategy(...)` — merge staged rows into the target dimension
6. return updated batch

`__cast_geometry_columns` is a workaround: GizmoSQL's ADBC driver ingests
geometry columns as BLOB, so it re-casts them via `ST_GeomFromWKB`.

### `MergeStrategy` (schema)
`aau_ais_schema/merge_strategies/` — `MergeStrategy` is a
`Callable[[Connection, Table, str, str, dict[str, str]], Table]`. Three
implementations:
- `SmartKeyMergeStrategy`
- `SurrogateKeyInsertStrategy`
- `SurrogateKeyMergeStrategy`

### Dimensions (schema)
`aau_ais_schema/dim/*_dim.py` — one class per dimension (Vessel, Date, Time,
Country, VesselType, VesselName, VesselConfig, CallSign, CargoType, Destination,
PosType, TransponderType, TrajType, TrajStateChange, TrajGeom, StopGeom,
GapType/Explanation/ImpMethod/ImpGeom, …). Many also provide `*IdExpander`
pre-processors (e.g. `DateIdExpander`, `TimeIdExpander`, `TrajCustFieldExpander`).

### Fact loading (traj)
- `load_traj_fact.load(src_id, dst_con, tbl)` — filters `type = 'in motion'`,
  assigns surrogate `ais_traj_id` (max+row_number), adds `load_id`,
  coalesces `prev_obj_type`, loads every dimension (with `name_map`s for
  start/end date/time/destination), then ingests the fact via the Jinja template
  `ais_obj_fact_load.sql.jinja2` (`adbc_ingest`, mode `append`, chunked 10k).
  Asserts the row count is preserved after dimension expansion.
- `load_stop_fact` — analogous for stationary/stop rows (`ais_stop_id`).
- `traj/utils.py` — `get_spatial_con()`, `is_traj_file()`, `TRAJ_FILE_COLUMNS`.
- `traj/__init__.py` — `JINJA_ENV = Environment(loader=PackageLoader("aau_ais_traj"))`.

## SQL Migrations Convention

Location: `packages/schema/src/aau_ais_schema/sql/`.

- `migrations/vNNN_*.sql` — **create** migrations; applied **ascending** on `db create`.
- `migrations/uNNN_*.sql` — **drop** migrations; applied **descending** on `db drop`.
- `compaction.sql` — DuckLake checkpoint (expire/merge/cleanup), run by `db compress`.

The `v`/`u` prefixes are how the CLI selects which migrations to run
(`file.name.startswith("v"/"u")`), so **naming matters** — keep the `v`/`u`
prefix and zero-padded numbering.

## Settings & Environment

- `aau_ais_core/settings.py`:
  - `GizmoSqlConnectionSettings` (host/port/user/`SecretStr` password/tls/…) with
    `connect()` (ADB `dbapi.connect`, `grpc`/`grpc+tls` URI).
  - `DataWarehouseConnectionSettings` (PostgreSQL `conn_str`).
  - `Settings(BaseSettings)` — `SettingsConfigDict` reads a `.env` file,
    `secrets_dir`, `case_sensitive=False`, `env_nested_delimiter="__"`;
    `Settings.create()` is `@cache`-ed.
- `aau_ais_cli/settings.py`: `Settings(CoreSettings)` adds `gizmosql`.
- **`.env`** lives at `packages/cli/.env`. The compose file references env vars
  `GIZMOSQL__USER`, `GIZMOSQL__PASSWORD`, `GIZMOSQL__PORT` (default `31337`),
  `GIZMOSQL_IP` (default `127.0.0.1`).

### Dev Docker stack
`packages/cli/src/aau_ais_cli/docker/compose.dev.yaml` runs
`gizmodata/gizmosql:v1.22.5-slim` (container `aau_ais_gizmosql`), init.sql on
mount, volume `gizmosql_data` at `/opt/gizmosql/data`, port `31337`.
`dev start`/`stop` invoke `docker compose` against this file.

## Conventions to Follow

- **Type hints everywhere** (the repo uses modern typing: `X | None`, `dict`, etc.).
- **ADB/`Connection` cursors** are used for all SQL execution
  (`with con.cursor() as cur:` / `cur.execute(..., parameters=[...])`).
  `rich` is used for colored CLI output.
- **SQL strings** often carry a `--sql` comment prefix; geometry columns may need
  the `__cast_geometry_columns` workaround.
- **Editable deps** — when adding a package, declare it in `[tool.uv.sources]`.
- **Naming**: snake_case modules; `_*_dim.py`, `_*_strategy.py`, `__*` private.
- Keep **migrations `v*`/`u*`** paired and in sync.
- Follow existing style; do not add comments unless asked.

## Known Issues / Caveats

- **`git` is not installed** in the environment (`git: command not found`); a
  `.git/` directory exists but history is not inspectable via the CLI. Treat the
  repo as non-git for tooling and infer state from files.
- **`lakehouse` script is likely broken**: `packages/traj/pyproject.toml`
  declares `lakehouse = "aau_traj.main:app"`, but the package is `aau_ais_traj`
  and has no `main` module. Verify before relying on it.
- **Stray file**: `packages/core/src/aau_ais_core/__init__ copy.py` looks like an
  accidental copy — do not import from it.
- **`aau-ais-schema/README.md` is empty.**
- **Geometry duplication across DuckDB/DuckLake** is a known, unsolved
  architectural problem (see `packages/traj/README.md`): no atomic
  cross-engine transactions, no cheap geometry equivalence/join key, and retry
  amplification causes duplicate spatial records. The `LoadContext`
  idempotency guard and the `__cast_geometry_columns` workaround are partial
  mitigations — be careful when touching fact/dimension load ordering.
