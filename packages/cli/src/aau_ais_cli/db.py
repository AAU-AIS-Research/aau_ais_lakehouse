from importlib import resources

import typer
from adbc_driver_gizmosql import dbapi
from rich import print
from typer import Typer

from aau_ais_cli.settings import Settings

cli = Typer()


@cli.command()
def create():
    """[green]Creates[/green] the schema :building_construction:"""
    settings = Settings.create()
    with (
        dbapi.connect(
            settings.gizmosql.uri, db_kwargs=settings.gizmosql.db_kwargs
        ) as con,
        con.cursor() as cur,
    ):
        q = resources.files("aau_ais_schema").joinpath("schema.sql").read_text()
        print("[green]Creating lakehouse schema[/green]...")
        cur.execute(q)
    print("[green]Lakehouse schema created successfully.[/green]")


@cli.command()
def drop():
    """[red]Drops[/red] the schema :litter_in_bin_sign:"""
    settings = Settings.create()
    with (
        dbapi.connect(
            settings.gizmosql.uri, db_kwargs=settings.gizmosql.db_kwargs
        ) as con,
        con.cursor() as cur,
    ):
        typer.confirm(
            "Are you sure you want to drop the database? All data will be lost!",
            abort=True,
        )
        q = resources.files("aau_ais_schema").joinpath("drop_schema.sql").read_text()
        print("[red]Dropping lakehouse schema[/red]...")
        cur.execute(q)
    print("[green]Lakehouse schema dropped successfully.[/green]")


@cli.command()
def compress():
    """Compresses the lakehouse schema by merging small files"""
    settings = Settings.create()
    with (
        dbapi.connect(
            settings.gizmosql.uri, db_kwargs=settings.gizmosql.db_kwargs
        ) as con,
        con.cursor() as cur,
    ):
        q = """--sql
call ducklake_expire_snapshots('lakehouse', older_than => now() - INTERVAL '1 hour');
call ducklake_merge_adjacent_files('lakehouse');
call ducklake_cleanup_old_files('lakehouse', cleanup_all => true);
"""
        cur.execute_update(q)
    print("[green]Checkpoint completed successfully.[/green]")
