import duckdb
from aau_ais_schema import LoadContext
from aau_ais_schema.dim import stop_geom_dim
from adbc_driver_gizmosql.dbapi import Connection
from duckdb import ColumnExpression, ConstantExpression, FunctionExpression
from pyarrow import Table

from aau_ais_traj import JINJA_ENV, __load_common, utils


def join_stop_geom_ids(src: Table, dst_con: Connection) -> Table:
    with utils.get_spatial_con() as con, con.begin():
        geom_col = ColumnExpression("geom")
        reader = (
            con.from_arrow(src)
            .select(
                ColumnExpression("ais_stop_id"),
                ColumnExpression("geom"),
                ColumnExpression("start_point"),
                ColumnExpression("end_point"),
                ColumnExpression("is_simple_geom"),
                ColumnExpression("is_valid_geom"),
                FunctionExpression("ST_Centroid", geom_col).alias("centroid"),
                FunctionExpression("ST_ConvexHull", geom_col).alias("simplified_geom"),
                FunctionExpression(
                    "ST_SimplifyPreserveTopology", geom_col, ConstantExpression(0.1)
                ).alias("simplified_geom_topo"),
            )
            .fetch_arrow_reader(500)
        )
        dim = stop_geom_dim.load(dst_con, reader)

        q = """--sql
select
    src.*,
    geom_id
from src
    inner join dim using (ais_stop_id);
"""
        return con.query(q).to_arrow_table()


def __get_max_fact_id(dst_con: Connection) -> int:
    with dst_con.cursor() as curs:
        return (
            curs.execute(
                "select max(ais_stop_id) from lakehouse.fact.ais_stop_fact;"
            ).fetchall()[0][0]
            or 0
        )


def __append_src(dst_con: Connection, tbl: Table, load_id: int):
    with duckdb.connect() as src_con, src_con.begin() as transaction:
        max_fact_id = __get_max_fact_id(dst_con)
        select = f"""--sql
{max_fact_id} + row_number() over ()    as ais_stop_id,
{load_id}                               as load_id,
*
"""
        return (
            transaction.from_arrow(tbl)
            .filter("type = 'stationary'")
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
        q = temp.render(fact_key="ais_stop_id", src_tbl="src")
        data = src_con.query(q).to_arrow_table().to_reader(max_chunksize=10000)

        curs.adbc_ingest(
            "ais_stop_fact",
            data,
            catalog_name="lakehouse",
            db_schema_name="fact",
            mode="append",
        )
        dst_con.commit()


def load(src_id: str, dst_con: Connection, tbl: Table):
    with LoadContext(src_id, "lakehouse.fact.ais_stop_fact", dst_con) as ctx:
        tbl = __append_src(dst_con, tbl, ctx.id)

        ctx.ingest_started()
        __load_common.load_date_dim(tbl, dst_con, "start_date_id")
        __load_common.load_date_dim(tbl, dst_con, "end_date_id")
        __load_common.load_time_dim(tbl, dst_con, "start_time_id")
        __load_common.load_time_dim(tbl, dst_con, "end_time_id")
        __load_common.load_country_dim(tbl, dst_con)

        tbl = __load_common.join_transponder_type_ids(tbl, dst_con)
        tbl = __load_common.join_post_type_ids(tbl, dst_con)
        tbl = __load_common.join_vessel_name_ids(tbl, dst_con)
        tbl = __load_common.join_vessel_type_ids(tbl, dst_con)
        tbl = __load_common.join_vessel_ids(tbl, dst_con)
        tbl = __load_common.join_traj_type_ids(tbl, dst_con)
        tbl = __load_common.join_vessel_config_ids(tbl, dst_con)
        tbl = __load_common.join_traj_state_change_ids(tbl, dst_con)
        tbl = __load_common.join_call_sign_ids(tbl, dst_con)
        tbl = __load_common.join_cargo_type_ids(tbl, dst_con)
        tbl = __load_common.join_destination_dim_ids(
            tbl,
            dst_con,
            {
                "org_msg": "start_destination_msg",
                "destination_id": "start_destination_id",
            },
        )
        tbl = __load_common.join_destination_dim_ids(
            tbl,
            dst_con,
            {
                "org_msg": "end_destination_msg",
                "destination_id": "end_destination_id",
            },
        )
        tbl = join_stop_geom_ids(tbl, dst_con)
        dst_con.commit()

        __load_fact(dst_con, tbl)

        ctx.ingest_stopped()


# import duckdb
# from adbc_driver_gizmosql.dbapi import Connection
# from duckdb import (
#     ColumnExpression,
#     ConstantExpression,
#     DuckDBPyConnection,
#     FunctionExpression,
# )
# from pyarrow import Table

# from aisdk_traj_lakehouse import JINJA_ENV, __load_common, utils
# from aisdk_traj_lakehouse.dim import stop_geom_dim
# from aisdk_traj_lakehouse.load_context import LoadContext


# def get_stop_geom_ids(data: Table, dst_con: Connection) -> Table:
#     with utils.get_spatial_con() as con:
#         geom_col = ColumnExpression("geom")
#         reader = (
#             con.from_arrow(data)
#             .select(
#                 ColumnExpression("row_id"),
#                 ColumnExpression("geom"),
#                 ColumnExpression("start_point"),
#                 ColumnExpression("end_point"),
#                 ColumnExpression("is_simple_geom"),
#                 ColumnExpression("is_valid_geom"),
#                 FunctionExpression("ST_Centroid", geom_col).alias("centroid"),
#                 FunctionExpression("ST_ConvexHull", geom_col).alias("simplified_geom"),
#                 FunctionExpression(
#                     "ST_SimplifyPreserveTopology", geom_col, ConstantExpression(0.1)
#                 ).alias("simplified_geom_topo"),
#             )
#             .fetch_arrow_reader(500)
#         )
#         return stop_geom_dim.load(dst_con, reader)


# def load(src_id: str, dst_con: Connection, tbl: Table):
#     with (
#         utils.get_spatial_con() as src_con,
#         dst_con.cursor() as cursor,
#         LoadContext(src_id, "lakehouse.fact.ais_stop_fact", dst_con) as ctx,
#     ):
#         max_id: int = (
#             cursor.execute(
#                 "select max(ais_stop_id) from lakehouse.fact.ais_stop_fact;"
#             ).fetchall()[0][0]
#             or 0
#         )
#         src_con.execute("call register_geoarrow_extensions();")

#         tbl = (
#             src_con.from_arrow(tbl)
#             .filter("type = 'stationary'")
#             .project("row_number() OVER () AS row_id, *")
#             .fetch_arrow_table()
#         )
#         transponder_type_ids = __load_common.get_transponder_type_ids(tbl, dst_con)
#         vessel_name_ids = __load_common.get_vessel_name_ids(tbl, dst_con)
#         vessel_type_ids = __load_common.get_vessel_type_ids(tbl, dst_con)
#         vessel_ids = __load_common.get_vessel_ids(tbl, dst_con)
#         traj_type_ids = __load_common.get_traj_type_ids(tbl, dst_con)
#         vessel_config_ids = __load_common.get_vessel_config_ids(tbl, dst_con)
#         traj_state_change_ids = __load_common.get_traj_state_change_ids(tbl, dst_con)
#         stop_geom_ids = get_stop_geom_ids(tbl, dst_con)

#         ctx.ingest_started()

#         temp = JINJA_ENV.get_template("ais_stop_fact_load.sql.jinja2")
#         q = temp.render(src_tbl="tbl")
#         data = (
#             src_con.query(q, params={"load_id": ctx.id, "max_ais_stop_id": max_id})
#             .to_arrow_table()
#             .to_reader(max_chunksize=10000)
#         )

#         cursor.adbc_ingest(
#             "ais_stop_fact",
#             data,
#             catalog_name="lakehouse",
#             db_schema_name="fact",
#             mode="append",
#         )
#         ctx.ingest_stopped()
