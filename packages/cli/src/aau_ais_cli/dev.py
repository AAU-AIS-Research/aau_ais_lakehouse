import subprocess
import time
from importlib import resources
from pathlib import Path

import typer
from adbc_driver_gizmosql import dbapi
from rich import print

from aau_ais_cli.settings import Settings

typer = typer.Typer()

COMPOSE_FILE = Path(__file__).parents[2] / "compose.dev.yaml"


def get_settings():
    return Settings()  # type: ignore


@typer.command()
def start():
    """Start the development docker stack"""
    cmd = ["docker", "compose", "-f", COMPOSE_FILE.as_posix(), "up", "-d"]
    print("Starting services...")

    try:
        subprocess.run(
            cmd,
            capture_output=True,
            check=True,
            text=True,
        )
        time.sleep(3)
    except subprocess.CalledProcessError as e:
        print("--- DOCKER ERROR ---")
        print(e.stderr)  # This contains the actual reason Docker failed
        print("--------------------")

    settings = get_settings()
    with dbapi.connect(
        settings.gizmosql.uri, db_kwargs=settings.gizmosql.db_kwargs
    ) as con:
        q = resources.files("aau_ais_schema").joinpath("schema.sql").read_text()

        print("Executing SQL query to create lakehouse schema...")
        with con.cursor() as cur:
            cur.execute(q)
    print("[green]Lakehouse schema initialized successfully.[/green]")


@typer.command()
def stop():
    """Stop the development docker stack"""
    cmd = ["docker", "compose", "-f", COMPOSE_FILE, "down"]
    print("stopping services...")
    subprocess.run(cmd)
