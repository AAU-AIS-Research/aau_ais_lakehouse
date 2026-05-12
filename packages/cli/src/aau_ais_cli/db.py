from importlib import resources

import typer
from rich import print
from typer import Typer

from aau_ais_cli import AISContext

cli = Typer()


@cli.command()
def create(ctx: AISContext):
    """[green]Creates[/green] the schema :building_construction:"""
    settings = ctx.obj

    with (
        settings.gizmosql.connect() as con,
        con.cursor() as cur,
    ):
        q = (
            resources.files("aau_ais_schema")
            .joinpath("sql", "create_schema.sql")
            .read_text()
        )
        print("[green]Creating lakehouse schema[/green]...")
        cur.execute(q)
        con.commit()
    print("[green]Lakehouse schema created successfully.[/green]")


@cli.command()
def drop(ctx: AISContext):
    """[red]Drops[/red] the schema :litter_in_bin_sign:"""
    settings = ctx.obj
    with (
        settings.gizmosql.connect() as con,
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
        con.commit()
    print("[green]Lakehouse schema dropped successfully.[/green]")


@cli.command()
def compress(ctx: AISContext):
    """Compresses the lakehouse schema by merging small files"""
    settings = ctx.obj
    with (
        settings.gizmosql.connect() as con,
        con.cursor() as cur,
    ):
        q = (
            resources.files("aau_ais_schema")
            .joinpath("sql", "compaction.sql")
            .read_text()
        )
        cur.executescript(q)
        con.commit()
    print("[green]Checkpoint completed successfully.[/green]")
