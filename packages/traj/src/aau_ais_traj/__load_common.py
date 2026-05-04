import duckdb
from aau_ais_schema.dim import (
    date_dim,
    time_dim,
    traj_state_change_dim,
    traj_type_dim,
)
from adbc_driver_gizmosql.dbapi import Connection
from duckdb import ColumnExpression, DuckDBPyConnection
from pyarrow import Table


def to_tbl(con: DuckDBPyConnection, target_tbl: str, tbl: Table):
    q = f"""--sql
create temporary table {target_tbl} as (
    select
        row_number() over () as row_id,
        *,
        hash(
            list_transform(
                ST_Dump(ST_Points(geom)),
                lambda p: (round(st_x(p.geom), 5), round(st_y(p.geom), 5), start_date_id, start_time_id, end_date_id, end_time_id)
            )
        ) as geom_hash
    from tbl
);
"""
    con.execute(q)


def join_traj_type_ids(src: Table, dst_con: Connection) -> Table:
    with duckdb.connect() as con, con.begin():
        reader = (
            con.from_arrow(src)
            .select("prev_obj_type as traj_type")
            .distinct()
            .fetch_arrow_reader()
        )
        dim = traj_type_dim.load(dst_con, reader)

        q = """--sql
select
    src.*,
    dim.traj_type_id as prev_obj_type_id
from src
    inner join dim on prev_obj_type is not distinct from dim.traj_type;
"""
        return con.query(q).to_arrow_table()


def join_traj_state_change_ids(src: Table, dst_con: Connection) -> Table:
    with duckdb.connect() as con, con.begin():
        reader = (
            con.from_arrow(src).select("state_change").distinct().fetch_arrow_reader()
        )
        dim = traj_state_change_dim.load(dst_con, reader)

        q = """--sql
select
    src.*,
    state_change_id
from src
    inner join dim using (state_change);
"""
        return con.query(q).to_arrow_table()


def load_date_dim(src: Table, dst_con: Connection, date_id_name: str):
    col = ColumnExpression(date_id_name).alias("date_id")
    with duckdb.connect() as con:
        reader = con.from_arrow(src).select(col).distinct().fetch_arrow_reader()
        date_dim.load(dst_con, reader)


def load_time_dim(src: Table, dst_con: Connection, time_id_name: str):
    col = ColumnExpression(time_id_name).alias("time_id")
    with duckdb.connect() as con:
        reader = con.from_arrow(src).select(col).distinct().fetch_arrow_reader()
        time_dim.load(dst_con, reader)
