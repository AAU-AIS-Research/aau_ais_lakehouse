from adbc_driver_manager.dbapi import Connection

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SurrogateKeyMergeStrategy


class VesselConfigDim(Dimension):
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "vessel_config_dim",
        pre_processors: list[Processor] = [],
    ):
        surrogate = "vessel_config_id"
        keys = [
            "length",
            "width",
            "height",
            "max_draught",
            "dwt",
            "grt",
            "to_bow",
            "to_stern",
            "to_port",
            "to_starboard",
            "main_engine_kwh",
            "aux_engine_kwh",
        ]

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
