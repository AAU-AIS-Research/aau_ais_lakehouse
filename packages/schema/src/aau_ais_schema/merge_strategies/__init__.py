from collections.abc import Callable

from adbc_driver_manager.dbapi import Connection
from pyarrow import Table

from ._smart_key_merge_strategy import SmartKeyMergeStrategy
from ._surrogate_key_insert_strategy import SurrogateKeyInsertStrategy
from ._surrogate_key_merge_strategy import SurrogateKeyMergeStrategy

MergeStrategy = Callable[[Connection, Table, str, str, dict[str, str]], Table]

__all__ = [
    "SmartKeyMergeStrategy",
    "SurrogateKeyInsertStrategy",
    "SurrogateKeyMergeStrategy",
    "MergeStrategy",
]
