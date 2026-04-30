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
        q = (
            resources.files("aau_ais_schema")
            .joinpath("sql", "create_schema.sql")
            .read_text()
        )
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
        q = (
            resources.files("aau_ais_schema")
            .joinpath("sql", "drop_schema.sql")
            .read_text()
        )
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
        q = (
            resources.files("aau_ais_schema")
            .joinpath("sql", "compaction.sql")
            .read_text()
        )
        cur.executescript(q)
    print("[green]Checkpoint completed successfully.[/green]")
