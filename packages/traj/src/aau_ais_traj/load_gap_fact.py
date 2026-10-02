import logging

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from aau_ais_schema import LoadContext
from aau_ais_schema.dim import (
    DateDim,
    DateIdExpander,
    GapTypeDim,
    TimeDim,
    TimeIdExpander,
    TrajStateChangeDim,
    TransponderTypeDim,
    VesselTypeDim,
)
from adbc_driver_manager.dbapi import Connection
from pyarrow import Table

from aau_ais_traj import JINJA_ENV

logger = logging.getLogger(__name__)


def __get_traj_keys(dst_con: Connection, src: Table):
    with dst_con.cursor() as curs:
        abv = src.select(
            [
                "start_date_id",
                "start_time_id",
                "end_date_id",
                "end_time_id",
                "mmsi",
                "start_type",
                "end_type",
            ]
        )
        curs.adbc_ingest("vessel_id_merge_test", abv, "replace", temporary=True)

        q = """
    with traj as (
        select
            ais_traj_id,
            vessel_id,
            mmsi,
            start_date_id,
            end_date_id,
            start_time_id,
            end_time_id
        from lakehouse.fact.ais_traj_fact
        inner join dim.vessel_dim using (vessel_id)
        where start_date_id between $1 and $2
            and end_date_id between $1 and $2
    ), stop as (
        select
            ais_stop_id,
            vessel_id,
            mmsi,
            start_date_id,
            end_date_id,
            start_time_id,
            end_time_id
        from lakehouse.fact.ais_stop_fact
        inner join dim.vessel_dim using (vessel_id)
        where start_date_id between $1 and $2
            and end_date_id between $1 and $2
    )
    select 
        src.start_date_id,
        src.start_time_id,
        src.end_date_id,
        src.end_time_id,
        src.mmsi,
        coalesce(
            start_traj.vessel_id,
            start_stop.vessel_id
        )                           as vessel_id,
        start_traj.ais_traj_id      as start_traj_id,
        start_stop.ais_stop_id      as start_stop_id,
        end_traj.ais_traj_id        as end_traj_id,
        end_stop.ais_stop_id        as end_stop_id
    from vessel_id_merge_test as src
    left outer join traj as start_traj 
        on src.start_type = 'in motion'
        and src.mmsi = start_traj.mmsi
        and src.start_date_id = start_traj.end_date_id
        and src.start_time_id = start_traj.end_time_id
    left outer join stop as start_stop
        on src.start_type = 'stationary'
        and src.mmsi = start_stop.mmsi
        and src.start_date_id = start_stop.end_date_id
        and src.start_time_id = start_stop.end_time_id
    left outer join traj as end_traj 
        on src.end_type = 'in motion'
        and src.mmsi = end_traj.mmsi
        and src.end_date_id = end_traj.start_date_id
        and src.end_time_id = end_traj.start_time_id
    left outer join stop as end_stop
        on src.end_type = 'stationary'
        and src.mmsi = end_stop.mmsi
        and src.end_date_id = end_stop.start_date_id
        and src.end_time_id = end_stop.start_time_id;
"""
        return curs.execute(
            q, parameters=[pc.min(src["start_date_id"]), pc.max(src["end_date_id"])]
        ).fetch_arrow_table()


def __join_traj_keys(dst_con: Connection, tbl: Table) -> Table:
    traj_table = __get_traj_keys(dst_con, tbl)
    q = """
select *
from tbl
    inner join traj_table using (start_date_id, start_time_id, end_date_id, end_time_id, mmsi);
"""
    return duckdb.query(q).to_arrow_table()


def __get_max_fact_id(dst_con: Connection) -> int:
    with dst_con.cursor() as curs:
        return (
            curs.execute(
                "select max(ais_gap_id) from lakehouse.fact.ais_gap_fact;"
            ).fetchall()[0][0]
            or 0
        )


def __append_src(dst_con: Connection, tbl: Table, load_id: int) -> Table:
    max_fact_id = __get_max_fact_id(dst_con)
    gap_id = pa.array(
        range(max_fact_id + 1, max_fact_id + len(tbl) + 1), type=pa.uint64()
    )
    tbl.append_column("ais_gap_id", gap_id)
    select_stmt = f"""
{max_fact_id} + row_number() over ()    as ais_gap_id,
{load_id}                               as load_id,
*
"""
    return duckdb.from_arrow(tbl).select(select_stmt).to_arrow_table()


def __gap_type_preprocessor(src: Table, name_map: dict[str, str]) -> Table:
    src_len = len(src)
    return src.append_column(
        name_map["long_name"], pa.nulls(src_len, type=pa.string())
    ).append_column(name_map["dsc"], pa.nulls(src_len, type=pa.string()))


def __load_fact(dst_con: Connection, src: Table, load_id: int) -> None:
    with (
        dst_con.cursor() as curs,
        duckdb.connect() as src_con,
        src_con.begin(),
    ):
        src_con.load_extension("spatial")

        temp = JINJA_ENV.get_template("ais_gap_fact_load.sql.jinja2")
        q = temp.render(src_tbl="src")
        data = src_con.query(q).to_arrow_reader()

        curs.adbc_ingest(
            "ais_gap_fact",
            data,
            catalog_name="lakehouse",
            db_schema_name="fact",
            mode="append",
        )
        dst_con.commit()


def load(src_id: str, dst_con: Connection, tbl: Table):
    with LoadContext(src_id, "lakehouse.fact.ais_gap_fact", dst_con) as ctx:
        start_tbl_length = len(tbl)
        tbl = __append_src(dst_con, tbl, ctx.id)

        ctx.ingest_started()

        # Date Dimension
        date_dim = DateDim(dst_con, pre_processors=[DateIdExpander()])
        date_dim.load(tbl, name_map={"date_id": "start_date_id"})
        date_dim.load(tbl, name_map={"date_id": "end_date_id"})

        # Time Dimension
        time_dim = TimeDim(dst_con, pre_processors=[TimeIdExpander()])
        time_dim.load(tbl, name_map={"time_id": "start_time_id"})
        time_dim.load(tbl, name_map={"time_id": "end_time_id"})

        tbl = __join_traj_keys(dst_con, tbl)
        tbl = TransponderTypeDim(dst_con).load(tbl)
        tbl = VesselTypeDim(dst_con).load(tbl)
        tbl = TrajStateChangeDim(dst_con).load(tbl)  # what if midnight
        tbl = GapTypeDim(dst_con, pre_processors=[__gap_type_preprocessor]).load(
            tbl, name_map={"short_name": "gap_type"}
        )

        # Set the default "empty" gap_imp_id in the source data
        gap_imp_id = dst_con.execute(
            "select gap_imp_id from ais.dim.gap_imp_mv_dim where method_name = 'empty'"
        ).fetchall()[0][0]
        tbl = tbl.append_column(
            "gap_imp_id", pa.array([gap_imp_id] * len(tbl), type=pa.int32())
        )

        assert len(tbl) - start_tbl_length == 0, (
            f"Source length: {start_tbl_length} is not equal to table length after processing: {len(tbl)}"
        )
        logger.info("Committing dimension table ingest")
        dst_con.commit()
        logger.info("Dimension ingestion committed")

        __load_fact(dst_con, tbl, ctx.id)
        ctx.ingest_stopped()
