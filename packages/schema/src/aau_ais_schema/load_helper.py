import duckdb
from adbc_driver_manager.dbapi import Connection
from duckdb import ColumnExpression
from pyarrow import Table

from aau_ais_schema.dim import (
    call_sign_dim,
    cargo_type_dim,
    destination_dim,
    nav_status_dim,
    pos_type_dim,
    transponder_type_dim,
    vessel_dim,
    vessel_name_dim,
    vessel_type_dim,
)


def join_vessel_ids(src: Table, dst_con: Connection) -> Table:
    with duckdb.connect() as con, con.begin():
        reader = (
            con.from_arrow(src)
            .select(
                "mmsi",
                "imo",
                "mid",
                "radio_service_type",
                "is_valid_mmsi",
                "is_valid_imo",
                "in_eu_mrv_db",
            )
            .distinct()
            .fetch_arrow_reader()
        )
        dim = vessel_dim.load(dst_con, reader)
        q = """--sql
select
    src.*,
    vessel_id
from src
    inner join dim on
        src.mmsi = dim.mmsi
        and src.imo is not distinct from dim.imo;
"""
        return con.query(q).to_arrow_table()


def join_transponder_type_ids(src: Table, dst_con: Connection) -> Table:
    with duckdb.connect() as con, con.begin():
        reader = (
            con.from_arrow(src)
            .select("transponder_type")
            .distinct()
            .fetch_arrow_reader()
        )
        dim = transponder_type_dim.load(dst_con, reader)
        q = """--sql
select
    src.*,
    transponder_type_id
from src
    inner join dim using (transponder_type);
"""
        return con.query(q).to_arrow_table()


def join_vessel_type_ids(
    src: Table, dst_con: Connection, name_map: dict[str, str] = {}
) -> Table:
    names = {"vessel_type_id": "vessel_type_id", "vessel_type": "vessel_type"}
    names.update(name_map)
    join_col = ColumnExpression(names["vessel_type"]).alias("vessel_type")

    with duckdb.connect() as con, con.begin():
        reader = con.from_arrow(src).select(join_col).distinct().fetch_arrow_reader()
        dim = vessel_type_dim.load(dst_con, reader)
        q = f"""--sql
select
    src.*,
    vessel_type_id as {names["vessel_type_id"]}
from src
    inner join dim on dim.vessel_type = src.{names["vessel_type"]};
"""
        return con.query(q).to_arrow_table()


def join_vessel_name_ids(
    src: Table, dst_con: Connection, name_map: dict[str, str] = {}
) -> Table:
    names = {"vessel_name_id": "vessel_name_id", "vessel_name": "vessel_name"}
    names.update(name_map)
    join_col = ColumnExpression(names["vessel_name"]).alias("vessel_name")

    with duckdb.connect() as con, con.begin():
        reader = con.from_arrow(src).select(join_col).distinct().fetch_arrow_reader()
        dim = vessel_name_dim.load(dst_con, reader)

        q = f"""--sql
select
    src.*,
    vessel_name_id as {names["vessel_name_id"]}
from src
    inner join dim on dim.vessel_name = src.{names["vessel_name"]};
"""
        return con.query(q).to_arrow_table()


def join_pos_type_ids(
    src: Table, dst_con: Connection, name_map: dict[str, str] = {}
) -> Table:
    names = {"pos_type_id": "pos_type_id", "pos_type": "pos_type"}
    names.update(name_map)

    join_col = ColumnExpression(names["pos_type"]).alias("pos_type")

    with duckdb.connect() as con, con.begin():
        reader = con.from_arrow(src).select(join_col).distinct().fetch_arrow_reader()
        dim = pos_type_dim.load(dst_con, reader)

        q = f"""--sql
select
    src.*,
    pos_type_id as {names["pos_type_id"]}
from src
    inner join dim on src.{names["pos_type"]} = dim.pos_type;
"""
        return con.query(q).to_arrow_table()


def join_cargo_type_ids(src: Table, dst_con: Connection) -> Table:
    with duckdb.connect() as con, con.begin():
        reader = (
            con.from_arrow(src).select("cargo_type").distinct().fetch_arrow_reader()
        )
        dim = cargo_type_dim.load(dst_con, reader)

        q = """--sql
select
    src.*,
    cargo_type_id
from src
    inner join dim using (cargo_type);
"""
        return con.query(q).to_arrow_table()


def join_call_sign_ids(src: Table, dst_con: Connection) -> Table:
    with duckdb.connect() as con, con.begin():
        reader = con.from_arrow(src).select("call_sign").distinct().fetch_arrow_reader()
        dim = call_sign_dim.load(dst_con, reader)

        q = """--sql
select
    src.*,
    call_sign_id
from src
    inner join dim using (call_sign);
"""
        return con.query(q).to_arrow_table()


def join_destination_dim_ids(
    src: Table, dst_con: Connection, name_map: dict[str, str] = {}
) -> Table:
    names = {"destination_id": "destination_id", "org_msg": "org_msg"}
    names.update(name_map)

    join_col = ColumnExpression(names["org_msg"]).alias("org_msg")

    with duckdb.connect() as con, con.begin():
        reader = con.from_arrow(src).select(join_col).distinct().fetch_arrow_reader()
        dim = destination_dim.load(dst_con, reader)

        q = f"""--sql
select
    src.*,
    destination_id as {names["destination_id"]}
from src
    inner join dim on src.{names["org_msg"]} = dim.org_msg;
"""
        return con.query(q).to_arrow_table()


def join_nav_status_dim_ids(
    src: Table, dst_con: Connection, name_map: dict[str, str] = {}
) -> Table:
    names = {"nav_status_id": "nav_status_id", "nav_status": "nav_status"}
    names.update(name_map)
    join_col = ColumnExpression(names["nav_status"]).alias("nav_status")

    with duckdb.connect() as con, con.begin():
        reader = con.from_arrow(src).select(join_col).distinct().fetch_arrow_reader()
        dim = nav_status_dim.load(dst_con, reader)

        q = f"""--sql
select
    src.*,
    nav_status_id as {names["nav_status_id"]}
from src
    inner join dim on dim.nav_status = src.{names["nav_status"]};
"""
        return con.query(q).to_arrow_table()
