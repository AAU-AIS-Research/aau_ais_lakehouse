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
        settings.gizmosql.connect(autocommit=True) as con,
        con.cursor() as cur,
    ):
        files = (
            file
            for file in resources.files("aau_ais_schema")
            .joinpath("sql", "migrations")
            .iterdir()
            if file.name.startswith("v")
        )

        for migration in sorted(files, key=lambda f: f.name):
            cur.execute(migration.read_text())
            print(f"[green]Migration {migration.name} done[/green]...")

    print("[green]Lakehouse schema created successfully.[/green]")


@cli.command()
def drop(ctx: AISContext):
    """[red]Drops[/red] the schema :litter_in_bin_sign:"""
    settings = ctx.obj
    with (
        settings.gizmosql.connect(autocommit=True) as con,
        con.cursor() as cur,
    ):
        typer.confirm(
            "Are you sure you want to drop the database? All data will be lost!",
            abort=True,
        )

        files = (
            file
            for file in resources.files("aau_ais_schema")
            .joinpath("sql", "migrations")
            .iterdir()
            if file.name.startswith("u")
        )
        for migration in sorted(files, key=lambda f: f.name, reverse=True):
            cur.execute(migration.read_text())
            print(f"[red]Migration {migration.name} done[/red]...")
    print("[green]Lakehouse schema dropped successfully.[/green]")


@cli.command()
def compress(ctx: AISContext):
    """Compresses the lakehouse schema by merging small files"""
    settings = ctx.obj
    with (
        settings.gizmosql.connect(autocommit=True) as con,
        con.cursor() as cur,
    ):
        q = (
            resources.files("aau_ais_schema")
            .joinpath("sql", "compaction.sql")
            .read_text()
        )
        cur.executescript(q)
    print("[green]Checkpoint completed successfully.[/green]")
