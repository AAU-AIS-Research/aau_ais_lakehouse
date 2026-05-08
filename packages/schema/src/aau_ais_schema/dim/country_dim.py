from adbc_driver_manager.dbapi import Connection

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SmartKeyMergeStrategy


class CountryDim(Dimension):
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "country_dim",
        pre_processors: list[Processor] = [],
    ) -> None:
        merge_strategy = SmartKeyMergeStrategy()
        columns = [
            "alpha2",
            "alpha3",
            "country_name",
            "country_code",
            "region",
            "sub_region",
            "intermediate_region",
            "region_code",
            "sub_region_code",
            "intermediate_region_code",
        ]
        super().__init__(
            con,
            catalog_name,
            schema_name,
            table_name,
            columns,
            merge_strategy,
            pre_processors,
        )
