from typer import Context

from aau_ais_cli.settings import Settings


class AISContext(Context):
    obj: Settings
