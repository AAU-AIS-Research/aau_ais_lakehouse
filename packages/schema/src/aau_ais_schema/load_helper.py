import duckdb
from adbc_driver_manager.dbapi import Connection
from duckdb import ColumnExpression
from pyarrow import Table

from aau_ais_schema.dim import (
    call_sign_dim,
    cargo_type_dim,
    country_dim,
    destination_dim,
    pos_type_dim,
    transponder_type_dim,
    vessel_config_dim,
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


def join_vessel_type_ids(src: Table, dst_con: Connection) -> Table:
    with duckdb.connect() as con, con.begin():
        reader = (
            con.from_arrow(src).select("vessel_type").distinct().fetch_arrow_reader()
        )
        dim = vessel_type_dim.load(dst_con, reader)
        q = """--sql
select
    src.*,
    vessel_type_id
from src
    inner join dim using (vessel_type);
"""
        return con.query(q).to_arrow_table()


def join_vessel_name_ids(src: Table, dst_con: Connection) -> Table:
    with duckdb.connect() as con, con.begin():
        reader = (
            con.from_arrow(src).select("vessel_name").distinct().fetch_arrow_reader()
        )
        dim = vessel_name_dim.load(dst_con, reader)

        q = """--sql
select
    src.*,
    vessel_name_id
from src
    inner join dim using (vessel_name);
"""
        return con.query(q).to_arrow_table()


def join_vessel_config_ids(src: Table, dst_con: Connection) -> Table:
    with duckdb.connect() as con, con.begin():
        reader = (
            con.from_arrow(src)
            .select(
                "length",
                "width",
                "height",
                "max_draught",
                "dwt",
                "grt",
                "to_bow",
                "to_stern",
                "to_port",
                "to_starboard",
                "main_engine_kwh",
                "aux_engine_kwh",
            )
            .distinct()
            .fetch_arrow_reader()
        )

        dim = vessel_config_dim.load(dst_con, reader)
        q = """--sql
select
    src.*,
    vessel_config_id
from src
    inner join dim as vcd on
        vcd.length              is not distinct from src.length
        and vcd.width           is not distinct from src.width
        and vcd.height          is not distinct from src.height
        and vcd.max_draught     is not distinct from src.max_draught
        and vcd.dwt             is not distinct from src.dwt
        and vcd.grt             is not distinct from src.grt
        and vcd.to_bow          is not distinct from src.to_bow
        and vcd.to_stern        is not distinct from src.to_stern
        and vcd.to_port         is not distinct from src.to_port
        and vcd.to_starboard    is not distinct from src.to_starboard
        and vcd.main_engine_kwh is not distinct from src.main_engine_kwh
        and vcd.aux_engine_kwh  is not distinct from src.aux_engine_kwh;
"""
        return con.query(q).to_arrow_table()


def join_pos_type_ids(src: Table, dst_con: Connection) -> Table:
    with duckdb.connect() as con, con.begin():
        reader = con.from_arrow(src).select("pos_type").distinct().fetch_arrow_reader()
        dim = pos_type_dim.load(dst_con, reader)

        q = """--sql
select
    src.*,
    pos_type_id
from src
    inner join dim using (pos_type);
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


def load_country_dim(src: Table, dst_con: Connection):

    with duckdb.connect() as con:
        reader = (
            con.from_arrow(src)
            .select(
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
            )
            .distinct()
            .fetch_arrow_reader()
        )
        country_dim.load(dst_con, reader)
