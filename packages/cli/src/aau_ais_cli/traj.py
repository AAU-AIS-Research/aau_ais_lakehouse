import time
from pathlib import Path
from typing import Annotated, Callable

from aau_ais_schema import LoadContext
from aau_ais_traj import load_stop_fact, load_traj_fact, utils
from adbc_driver_gizmosql import dbapi
from adbc_driver_gizmosql.dbapi import Connection
from pyarrow import Table
from rich import print
from typer import Argument, Typer

from aau_ais_cli import AISContext

LoadFunc = Callable[[str, Connection, Table], None]

cli = Typer()


def __load(
    trajectory_file: Path, dst_tbl: str, dst_con: Connection, load_func: LoadFunc
) -> None:
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


@cli.command()
def load(
    ctx: AISContext,
    traj_file: Annotated[
        list[Path],
        Argument(..., help="Path to the parquet file(s) containing trajectory data."),
    ],
) -> None:
    """Load trajectory data into the lakehouse."""

    settings = ctx.obj

    for f in traj_file:
        with (
            dbapi.connect(
                settings.gizmosql.uri,
                db_kwargs=settings.gizmosql.db_kwargs,
                autocommit=False,
            ) as con,
        ):
            print(f"Processing {f}...")
            if not utils.is_traj_file(f):
                print(f"[yellow]Skipping {f} as it is not compatible[/yellow]")
                continue
            __load(f, "lakehouse.fact.ais_traj_fact", con, load_traj_fact.load)
            __load(f, "lakehouse.fact.ais_stop_fact", con, load_stop_fact.load)


@cli.command(deprecated=True)
def load_dir(
    ctx: AISContext,
    dir: Annotated[
        Path, Argument(..., help="Path to the directory containing trajectory data.")
    ],
) -> None:
    """Load a directory of trajectory files into the lakehouse.
    [black]Deprecated, please use load with glob pattern, brace expansion or similar[/black]
    """
    s = time.perf_counter()
    files = list(dir.iterdir())
    load(ctx, files)
    print(f"Load took {time.perf_counter() - s} seconds")
