from aau_ais_core import duckdb_utils
from adbc_driver_manager.dbapi import Connection
from pyarrow import Table

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SurrogateKeyInsertStrategy


class StopGeomFieldExpander:
    def __call__(self, batch: Table, name_map: dict[str, str]) -> Table:
        q = f"""--sql
select
    ais_stop_id,
    {name_map["geom"]},
    {name_map["start_point"]},
    {name_map["end_point"]},
    {name_map["is_simple_geom"]},
    {name_map["is_valid_geom"]},
    ST_Centroid(geom)                       as centroid,
    ST_ConvexHull(geom)                     as simplified_geom,
    ST_SimplifyPreserveTopology(geom, 0.1)  as simplified_geom_topo
from batch;
"""
        with duckdb_utils.get_spatial_con().begin() as con:
            return con.query(q).to_arrow_table()


class StopGeomDim(Dimension):
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "stop_geom_dim",
        pre_processors: list[Processor] = [],
    ):
        surrogate = "geom_id"
        keys = ["ais_stop_id"]
        attributes = [
            "geom",
            "start_point",
            "end_point",
            "is_simple_geom",
            "is_valid_geom",
            "centroid",
            "simplified_geom",
            "simplified_geom_topo",
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
            max_ingest_chunk_size=1000000,
        )
