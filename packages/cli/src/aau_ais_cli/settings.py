from aau_ais_core.settings import GizmoSqlConnectionSettings
from aau_ais_core.settings import Settings as CoreSettings


class Settings(CoreSettings):
    gizmosql: GizmoSqlConnectionSettings
