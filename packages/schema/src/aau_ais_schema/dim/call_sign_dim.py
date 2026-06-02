from adbc_driver_manager.dbapi import Connection

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SurrogateKeyMergeStrategy
from aau_ais_schema import utils


class CallSignDim(Dimension):
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "call_sign_dim",
        pre_processors: list[Processor] = [],
    ):
        surrogate = "call_sign_id"
        keys = ["call_sign"]

        sequence = utils.generate_sequence_name(schema_name, table_name, surrogate)
        merge_strategy = SurrogateKeyMergeStrategy(sequence, surrogate, keys)
        super().__init__(
            con,
            catalog_name,
            schema_name,
            table_name,
            columns=keys,
            merge_strategy=merge_strategy,
            pre_processors=pre_processors,
        )
