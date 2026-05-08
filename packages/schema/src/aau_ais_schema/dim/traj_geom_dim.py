import duckdb
from adbc_driver_manager.dbapi import Connection
from pyarrow import Table

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SurrogateKeyInsertStrategy


class TrajCustFieldExpander:
    def __call__(self, batch: Table, name_map: dict[str, str]) -> Table:
        q = f"""--sql
select
    ais_traj_id,
    {name_map["geom"]},
    {name_map["start_point"]},
    {name_map["end_point"]},
    {name_map["is_simple_geom"]},
    {name_map["is_valid_geom"]},
    null::geometry as cust_simplified_geom_00,
    null::geometry as cust_simplified_geom_01,
    null::geometry as cust_simplified_geom_02,
    null::geometry as cust_simplified_geom_03,
    null::geometry as cust_simplified_geom_04
from batch;
"""
        with duckdb.connect().begin() as con:
            return con.query(q).to_arrow_table()


class TrajGeomDim(Dimension):
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "traj_geom_dim",
        pre_processors: list[Processor] = [],
    ):
        surrogate = "geom_id"
        keys = ["ais_traj_id"]
        attributes = [
            "geom",
            "start_point",
            "end_point",
            "is_simple_geom",
            "is_valid_geom",
            "cust_simplified_geom_00",
            "cust_simplified_geom_01",
            "cust_simplified_geom_02",
            "cust_simplified_geom_03",
            "cust_simplified_geom_04",
        ]

        merge_strategy = SurrogateKeyInsertStrategy(surrogate, keys, attributes)
        super().__init__(
            con,
            catalog_name,
            schema_name,
            table_name,
            columns=keys + attributes,
            merge_strategy=merge_strategy,
            pre_processors=pre_processors,
        )
