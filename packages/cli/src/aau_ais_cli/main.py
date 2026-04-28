from typer import Typer

from aau_ais_cli import db, dev, traj

app = Typer()
app.add_typer(dev.typer, name="dev", help="Development utilities")
app.add_typer(db.typer, name="db", help="Schema utilities")
app.add_typer(traj.typer, name="traj", help="Trajectory utilities")
