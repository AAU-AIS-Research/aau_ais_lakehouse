from adbc_driver_manager.dbapi import Connection

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SurrogateKeyMergeStrategy


class VesselDim(Dimension):
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "vessel_dim",
        pre_processors: list[Processor] = [],
    ):
        surrogate = "vessel_id"
        keys = ["mmsi", "imo"]
        attributes = [
            "mid",
            "radio_service_type",
            "is_valid_mmsi",
            "is_valid_imo",
            "in_eu_mrv_db",
        ]

        merge_strategy = SurrogateKeyMergeStrategy(surrogate, keys, attributes)
        super().__init__(
            con,
            catalog_name,
            schema_name,
            table_name,
            columns=keys + attributes,
            merge_strategy=merge_strategy,
            pre_processors=pre_processors,
        )
