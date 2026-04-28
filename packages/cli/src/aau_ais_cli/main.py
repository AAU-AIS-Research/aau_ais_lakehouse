from typer import Typer

from aau_ais_cli import db, dev, traj

app = Typer()
app.add_typer(
    db.cli,
    name="db",
    help="Database utilities such as [bright_cyan]create[/bright_cyan], [bright_cyan]drop[/bright_cyan], [bright_cyan]compress[/bright_cyan]",
)
app.add_typer(
    dev.cli,
    name="dev",
    help="Development utilities such as [bright_cyan]start[/bright_cyan] [bright_cyan]stop[/bright_cyan] dev database server",
)
app.add_typer(
    traj.cli,
    name="traj",
    help="Trajectory utilities such as [bright_cyan]load[/bright_cyan] and [bright_cyan]load-dir[/bright_cyan]",
)
