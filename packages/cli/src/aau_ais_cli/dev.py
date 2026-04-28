import os
import subprocess
import time
from pathlib import Path
from typing import Annotated

from adbc_driver_gizmosql import dbapi
from adbc_driver_manager import OperationalError
from rich import print
from typer import Option, Typer

from aau_ais_cli import db
from aau_ais_cli.settings import Settings

cli = Typer()

COMPOSE_FILE = Path(__file__).parents[2] / "compose.dev.yaml"


def _wait_for_gizmosql(
    settings: Settings, max_retries: int = 60, interval: float = 1.0
):
    """
    Blocks until the GizmoSQL service is ready to accept connections at the given host/port.
    """
    print(
        f"[yellow]Waiting for GizmoSQL at {settings.gizmosql.host}:{settings.gizmosql.port}...[/yellow]"
    )
    for attempt in range(max_retries):
        try:
            # Try to create a TCP connection
            with dbapi.connect(
                settings.gizmosql.uri, db_kwargs=settings.gizmosql.db_kwargs
            ) as _:
                print("[green]GizmoSQL is ready![/green]")
                return True
        except (OperationalError, OSError):
            time.sleep(interval)

    raise RuntimeError(
        f"GizmoSQL did not become ready within {max_retries * interval:.1f} seconds."
    )


@cli.command()
def start(
    public: Annotated[
        bool,
        Option(
            help="Use this flag if the database should hosted on 0.0.0.0 instead of localhost",
        ),
    ] = False,
):
    """Start the development docker stack"""
    cmd = ["docker", "compose", "-f", COMPOSE_FILE.as_posix(), "up", "-d"]
    print("Starting services...")
    if public:
        os.environ["GIZMOSQL_IP"] = "0.0.0.0"

    try:
        subprocess.run(
            cmd,
            # env=env,
            capture_output=True,
            check=True,
            text=True,
        )
        # TODO: Implement prober health check instead of sleep
        # time.sleep(3)
    except subprocess.CalledProcessError as e:
        print("--- DOCKER ERROR ---")
        print(e.stderr)  # This contains the actual reason Docker failed
        print("--------------------")

    settings = Settings.create()
    try:
        _wait_for_gizmosql(settings)
    except RuntimeError as e:
        print(f"[red]Error: {e}[/red]")
        print("[yellow]Stopping services due to failed health check.[/yellow]")
        stop()
        return
    db.create()


@cli.command()
def stop():
    """Stop the development docker stack"""
    cmd = ["docker", "compose", "-f", COMPOSE_FILE, "down"]
    print("stopping services...")
    subprocess.run(cmd)
