from aau_ais_core.settings import HardchargerConnectionSettings
from aau_ais_core.settings import Settings as CoreSettings


class Settings(CoreSettings):
    hardcharger: HardchargerConnectionSettings
