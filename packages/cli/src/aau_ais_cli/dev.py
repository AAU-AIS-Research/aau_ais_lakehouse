import os
import subprocess
import time
from pathlib import Path
from typing import Annotated

import typer
from rich import print
from typer import Option

from aau_ais_cli import db

typer = typer.Typer()

COMPOSE_FILE = Path(__file__).parents[2] / "compose.dev.yaml"


@typer.command()
def start(
    public: Annotated[
        bool,
        Option(
            help="Use this flag if the database should be visible on the host network",
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
        time.sleep(3)
    except subprocess.CalledProcessError as e:
        print("--- DOCKER ERROR ---")
        print(e.stderr)  # This contains the actual reason Docker failed
        print("--------------------")
    db.create()


@typer.command()
def stop():
    """Stop the development docker stack"""
    cmd = ["docker", "compose", "-f", COMPOSE_FILE, "down"]
    print("stopping services...")
    subprocess.run(cmd)
