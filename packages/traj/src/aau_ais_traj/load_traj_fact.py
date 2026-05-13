import duckdb
from aau_ais_schema import LoadContext
from aau_ais_schema.dim import (
    CallSignDim,
    CargoTypeDim,
    CountryDim,
    DateDim,
    DateIdExpander,
    DestinationDim,
    PosTypeDim,
    TimeDim,
    TimeIdExpander,
    TrajCustFieldExpander,
    TrajGeomDim,
    TrajStateChangeDim,
    TrajTypeDim,
    TransponderTypeDim,
    VesselConfigDim,
    VesselDim,
    VesselNameDim,
    VesselTypeDim,
)
from adbc_driver_manager.dbapi import Connection
from loguru import logger
from pyarrow import Table

from aau_ais_traj import JINJA_ENV


def __get_max_fact_id(dst_con: Connection) -> int:
    with dst_con.cursor() as curs:
        return (
            curs.execute(
                "select max(ais_traj_id) from lakehouse.fact.ais_traj_fact;"
            ).fetchall()[0][0]
            or 0
        )


def __append_src(dst_con: Connection, tbl: Table, load_id: int) -> Table:
    with duckdb.connect() as src_con, src_con.begin():
        max_fact_id = __get_max_fact_id(dst_con)
        select = f"""--sql
{max_fact_id} + row_number() over ()    as ais_traj_id,
{load_id}                               as load_id,
*
replace (coalesce(prev_obj_type, 'unknown') as prev_obj_type)
"""
        return (
            src_con.from_arrow(tbl)
            .filter("type = 'in motion'")
            .select(select)
            .fetch_arrow_table()
        )


def __load_fact(dst_con: Connection, src: Table) -> None:
    with (
        dst_con.cursor() as curs,
        duckdb.connect() as src_con,
        src_con.begin(),
    ):
        temp = JINJA_ENV.get_template("ais_obj_fact_load.sql.jinja2")
        q = temp.render(fact_key="ais_traj_id", src_tbl="src")
        data = src_con.query(q).to_arrow_table().to_reader(max_chunksize=10000)

        curs.adbc_ingest(
            "ais_traj_fact",
            data,
            catalog_name="lakehouse",
            db_schema_name="fact",
            mode="append",
        )
        dst_con.commit()


def load(src_id: str, dst_con: Connection, tbl: Table):
    with LoadContext(src_id, "lakehouse.fact.ais_traj_fact", dst_con) as ctx:
        tbl = __append_src(dst_con, tbl, ctx.id)
        start_tbl_length = len(tbl)

        ctx.ingest_started()
        # Date Dimension
        date_dim = DateDim(dst_con, pre_processors=[DateIdExpander()])
        date_dim.load(tbl, name_map={"date_id": "start_date_id"})
        date_dim.load(tbl, name_map={"date_id": "end_date_id"})

        # Time Dimension
        time_dim = TimeDim(dst_con, pre_processors=[TimeIdExpander()])
        time_dim.load(tbl, name_map={"time_id": "start_time_id"})
        time_dim.load(tbl, name_map={"time_id": "end_time_id"})

        CountryDim(dst_con).load(tbl)
        tbl = VesselConfigDim(dst_con).load(tbl)
        tbl = TrajTypeDim(dst_con).load(
            tbl,
            name_map={"traj_type": "prev_obj_type", "traj_type_id": "prev_obj_type_id"},
        )
        tbl = TrajStateChangeDim(dst_con).load(tbl)
        tbl = TransponderTypeDim(dst_con).load(tbl)
        tbl = PosTypeDim(dst_con).load(tbl)
        tbl = VesselNameDim(dst_con).load(tbl)
        tbl = VesselTypeDim(dst_con).load(tbl)
        tbl = VesselDim(dst_con).load(tbl)
        tbl = CallSignDim(dst_con).load(tbl)
        tbl = CargoTypeDim(dst_con).load(tbl)
        destination_dim = DestinationDim(dst_con)
        tbl = destination_dim.load(
            tbl,
            name_map={
                "org_msg": "start_destination_msg",
                "destination_id": "start_destination_id",
            },
        )
        tbl = destination_dim.load(
            tbl,
            name_map={
                "org_msg": "end_destination_msg",
                "destination_id": "end_destination_id",
            },
        )

        tbl = TrajGeomDim(dst_con, pre_processors=[TrajCustFieldExpander()]).load(tbl)

        assert len(tbl) - start_tbl_length == 0
        logger.info("Committing dimension table ingest")
        dst_con.commit()
        logger.info("Dimension ingestion committed")

        __load_fact(dst_con, tbl)
        ctx.ingest_stopped()
