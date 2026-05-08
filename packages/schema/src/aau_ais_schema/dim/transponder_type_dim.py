from adbc_driver_manager.dbapi import Connection

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SurrogateKeyMergeStrategy


class TransponderTypeDim(Dimension):
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "transponder_type_dim",
        pre_processors: list[Processor] = [],
    ):
        surrogate = "transponder_type_id"
        keys = ["transponder_type"]

        merge_strategy = SurrogateKeyMergeStrategy(surrogate, keys)
        super().__init__(
            con,
            catalog_name,
            schema_name,
            table_name,
            columns=keys,
            merge_strategy=merge_strategy,
            pre_processors=pre_processors,
        )
