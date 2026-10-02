from adbc_driver_manager.dbapi import Connection

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SurrogateKeyMergeStrategy


class GapTypeDim(Dimension):
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "gap_type_dim",
        pre_processors: list[Processor] = [],
    ):
        surrogate = "gap_type_id"
        keys = ["short_name"]
        attributes = ["long_name", "dsc"]

        sequence = f"{schema_name}.{table_name}_seq"
        merge_strategy = SurrogateKeyMergeStrategy(
            sequence, surrogate, keys, attributes
        )
        super().__init__(
            con,
            catalog_name,
            schema_name,
            table_name,
            columns=keys + attributes,
            merge_strategy=merge_strategy,
            pre_processors=pre_processors,
        )
