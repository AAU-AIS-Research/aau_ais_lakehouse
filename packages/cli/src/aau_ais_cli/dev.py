import os
import subprocess
import time
from pathlib import Path
from typing import Annotated

from rich import print
from typer import Option, Typer

from aau_ais_cli import db

cli = Typer()

COMPOSE_FILE = Path(__file__).parents[2] / "compose.dev.yaml"


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
        time.sleep(3)
    except subprocess.CalledProcessError as e:
        print("--- DOCKER ERROR ---")
        print(e.stderr)  # This contains the actual reason Docker failed
        print("--------------------")
    db.create()


@cli.command()
def stop():
    """Stop the development docker stack"""
    cmd = ["docker", "compose", "-f", COMPOSE_FILE, "down"]
    print("stopping services...")
    subprocess.run(cmd)
