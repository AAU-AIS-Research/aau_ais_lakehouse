from pathlib import Path
from typing import Annotated, Callable

import typer
from adbc_driver_gizmosql import dbapi
from adbc_driver_gizmosql.dbapi import Connection
from pyarrow import Table
from rich import print
from typer import Argument

from aau_traj import JINJA_ENV, __load_stop_fact, __load_traj_fact, utils
from aau_traj.load_context import LoadContext
from aau_traj.settings import Settings

LoadFunc = Callable[[str, Connection, Table], None]
app = typer.Typer()


def get_settings():
    return Settings()  # type: ignore


@app.command()
def init() -> None:
    """Initialize the lakehouse schema."""

    settings = get_settings()

    print("Aquiring connection to GizmoSQL...")
    with dbapi.connect(
        settings.gizmosql.uri, db_kwargs=settings.gizmosql.db_kwargs
    ) as con:
        q = JINJA_ENV.get_template("schema.sql").render()

        print("Executing SQL query to create lakehouse schema...")
        with con.cursor() as cur:
            cur.execute(q)
    print("[green]Lakehouse schema initialized successfully.[/green]")


def __load(
    trajectory_file: Path, dst_tbl: str, dst_con: Connection, load_func: LoadFunc
):
    with utils.get_spatial_con().begin() as local_con:
        if LoadContext.is_loaded(trajectory_file.name, dst_tbl, dst_con):
            print(
                f"[yellow]Data already loaded into {dst_tbl}. Skipping load.[/yellow]"
            )
        else:
            print("Loading trajectory data into the lakehouse...")
            in_motion_tbl = local_con.read_parquet(
                trajectory_file.as_posix()
            ).fetch_arrow_table()
            load_func(trajectory_file.name, dst_con, in_motion_tbl)
            print(f"[green]Data loaded into {dst_tbl} successfully.[/green]")


@app.command()
def load(
    trajectory_file: Annotated[
        Path, Argument(..., help="Path to the parquet file containing trajectory data.")
    ],
) -> None:
    """Load trajectory data into the lakehouse."""

    settings = get_settings()

    with (
        dbapi.connect(
            settings.gizmosql.uri,
            db_kwargs=settings.gizmosql.db_kwargs,
            autocommit=False,
        ) as con,
    ):
        __load(
            trajectory_file, "lakehouse.fact.ais_traj_fact", con, __load_traj_fact.load
        )
        __load(
            trajectory_file, "lakehouse.fact.ais_stop_fact", con, __load_stop_fact.load
        )


@app.command()
def load_dir(
    dir: Annotated[
        Path, Argument(..., help="Path to the directory containing trajectory data.")
    ],
):
    for file in dir.iterdir():
        if not utils.is_traj_file(file):
            print(f"Skipping {file} as it is not compatible")
            continue
        print(f"Processing {file}...")
        load(file)


if __name__ == "__main__":
    load(Path("/srv/versitygw/ukc/ais_traj/2026/20260101_traj_staging.pq"))
