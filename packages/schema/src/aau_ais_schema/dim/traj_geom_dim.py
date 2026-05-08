import time

import duckdb
import pyarrow.compute as pc
from adbc_driver_manager.dbapi import Connection
from loguru import logger
from pyarrow import RecordBatchReader, Table

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


def __ingest(data: RecordBatchReader, dst_con: Connection) -> Table:
    tmp_tbl = "src_traj_geom_dim"
    with dst_con.cursor() as curs:
        curs.execute(f"drop table if exists {tmp_tbl};")
        curs.adbc_ingest(tmp_tbl, data, mode="replace", temporary=True)

        q = f"""--sql
alter table {tmp_tbl}
    add column if not exists geom_id uinteger default nextval('dim.traj_geom_dim_geom_id_seq');
alter table {tmp_tbl}
    add column if not exists is_new boolean default true;

insert into dim.traj_geom_dim by name (
    select
        *
        exclude (
            ais_traj_id,
            is_new
        )
        replace (
            ST_GeomFromWkb(geom)                    as geom,
            ST_GeomFromWkb(start_point)             as start_point,
            ST_GeomFromWkb(end_point)               as end_point,
            ST_GeomFromText(cust_simplified_geom_00) as cust_simplified_geom_00,
            ST_GeomFromText(cust_simplified_geom_01) as cust_simplified_geom_01,
            ST_GeomFromText(cust_simplified_geom_02) as cust_simplified_geom_02,
            ST_GeomFromText(cust_simplified_geom_03) as cust_simplified_geom_03,
            ST_GeomFromText(cust_simplified_geom_04) as cust_simplified_geom_04,
        )
    from {tmp_tbl}
);
"""
        curs.execute(q)

        return curs.execute(
            f"select ais_traj_id, geom_id, is_new from {tmp_tbl};"
        ).fetch_arrow_table()


def load(dst_con: Connection, data: RecordBatchReader) -> Table:
    logger.info("Loading traj_geom_dim...")
    s = time.perf_counter()
    result = __ingest(data, dst_con)

    logger.info(
        "Inserted {:,} row(s) into traj_geom_dim in {:,.2f}s",
        len(result.filter(pc.field("is_new"))),
        time.perf_counter() - s,
    )
    return result
