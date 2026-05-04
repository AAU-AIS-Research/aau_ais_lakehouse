import duckdb
from aau_ais_schema import LoadContext, load_helper
from aau_ais_schema.dim import traj_geom_dim
from adbc_driver_gizmosql.dbapi import Connection
from duckdb import ConstantExpression
from pyarrow import Table

from aau_ais_traj import JINJA_ENV, __load_common, utils


def join_traj_geom_ids(src: Table, dst_con: Connection) -> Table:
    with utils.get_spatial_con() as con, con.begin():
        data = (
            con.from_arrow(src)
            .select(
                "ais_traj_id",
                "geom",
                "start_point",
                "end_point",
                "is_simple_geom",
                "is_valid_geom",
                ConstantExpression("LINESTRING EMPTY").alias("cust_simplified_geom_00"),
                ConstantExpression("LINESTRING EMPTY").alias("cust_simplified_geom_01"),
                ConstantExpression("LINESTRING EMPTY").alias("cust_simplified_geom_02"),
                ConstantExpression("LINESTRING EMPTY").alias("cust_simplified_geom_03"),
                ConstantExpression("LINESTRING EMPTY").alias("cust_simplified_geom_04"),
            )
            .fetch_arrow_reader(500)
        )

        dim = traj_geom_dim.load(dst_con, data)

        q = """--sql
select
    src.*,
    geom_id
from src
    inner join dim using (ais_traj_id);
"""
        return con.query(q).to_arrow_table()


def __get_max_fact_id(dst_con: Connection) -> int:
    with dst_con.cursor() as curs:
        return (
            curs.execute(
                "select max(ais_traj_id) from lakehouse.fact.ais_traj_fact;"
            ).fetchall()[0][0]
            or 0
        )


def __append_src(dst_con: Connection, tbl: Table, load_id: int):
    with duckdb.connect() as src_con, src_con.begin() as transaction:
        max_fact_id = __get_max_fact_id(dst_con)
        select = f"""--sql
{max_fact_id} + row_number() over ()    as ais_traj_id,
{load_id}                               as load_id,
*
"""
        return (
            transaction.from_arrow(tbl)
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

        ctx.ingest_started()
        __load_common.load_date_dim(tbl, dst_con, "start_date_id")
        __load_common.load_date_dim(tbl, dst_con, "end_date_id")
        __load_common.load_time_dim(tbl, dst_con, "start_time_id")
        __load_common.load_time_dim(tbl, dst_con, "end_time_id")
        load_helper.load_country_dim(tbl, dst_con)

        tbl = __load_common.join_traj_type_ids(tbl, dst_con)
        tbl = __load_common.join_traj_state_change_ids(tbl, dst_con)
        tbl = load_helper.join_transponder_type_ids(tbl, dst_con)
        tbl = load_helper.join_pos_type_ids(tbl, dst_con)
        tbl = load_helper.join_vessel_name_ids(tbl, dst_con)
        tbl = load_helper.join_vessel_type_ids(tbl, dst_con)
        tbl = load_helper.join_vessel_ids(tbl, dst_con)
        tbl = load_helper.join_vessel_config_ids(tbl, dst_con)
        tbl = load_helper.join_call_sign_ids(tbl, dst_con)
        tbl = load_helper.join_cargo_type_ids(tbl, dst_con)
        tbl = load_helper.join_destination_dim_ids(
            tbl,
            dst_con,
            {
                "org_msg": "start_destination_msg",
                "destination_id": "start_destination_id",
            },
        )
        tbl = load_helper.join_destination_dim_ids(
            tbl,
            dst_con,
            {
                "org_msg": "end_destination_msg",
                "destination_id": "end_destination_id",
            },
        )
        tbl = join_traj_geom_ids(tbl, dst_con)
        dst_con.commit()

        __load_fact(dst_con, tbl)

        ctx.ingest_stopped()
