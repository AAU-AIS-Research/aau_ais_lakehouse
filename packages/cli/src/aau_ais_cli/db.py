from importlib import resources

from adbc_driver_gizmosql import dbapi
from rich import print
from typer import Typer

from aau_ais_cli.settings import Settings

typer = Typer()


@typer.command()
def create():
    """Creates the schema"""
    settings = Settings.create()
    with dbapi.connect(
        settings.gizmosql.uri, db_kwargs=settings.gizmosql.db_kwargs
    ) as con:
        q = resources.files("aau_ais_schema").joinpath("schema.sql").read_text()

        print("Executing SQL query to create lakehouse schema...")
        with con.cursor() as cur:
            cur.execute(q)
    print("[green]Lakehouse schema initialized successfully.[/green]")


@typer.command()
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
