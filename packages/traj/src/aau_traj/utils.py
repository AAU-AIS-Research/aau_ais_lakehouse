from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection

TRAJ_FILE_COLUMNS = {
    "region_code",
    "ukc_end",
    "traj_id",
    "to_stern",
    "to_starboard",
    "start_date_id",
    "draught_min",
    "sog_avg",
    "meters_to_prev_obj",
    "ukc_avg",
    "alpha3",
    "calc_speed_end",
    "country_code",
    "draught_start",
    "ukc_min",
    "is_valid_mmsi",
    "aux_engine_kwh",
    "is_simple_geom",
    "no_points",
    "sog_start",
    "meters",
    "calc_speed_max",
    "is_valid_geom",
    "draught_avg",
    "pos_type",
    "calc_speed_start",
    "intermediate_region",
    "sog_max",
    "ukc_max",
    "cargo_type",
    "country_name",
    "start_point",
    "ukc_median",
    "to_bow",
    "start_time_id",
    "calc_speed_avg",
    "state_change",
    "end_point",
    "intermediate_region_code",
    "main_engine_kwh",
    "draught_max",
    "sog_min",
    "width",
    "seconds",
    "end_date_id",
    "sog_median",
    "to_port",
    "region",
    "alpha2",
    "height",
    "vessel_type",
    "start_destination_msg",
    "call_sign",
    "in_eu_mrv_db",
    "length",
    "mid",
    "is_valid_imo",
    "mmsi",
    "end_time_id",
    "calc_speed_min",
    "imo",
    "vessel_name",
    "end_destination_msg",
    "seconds_to_prev_obj",
    "prev_obj_type",
    "sub_region",
    "ukc_start",
    "type",
    "radio_service_type",
    "geom",
    "sub_region_code",
    "draught_end",
    "transponder_type",
    "draught_median",
    "dwt",
    "sog_end",
    "tortuosity",
    "max_draught",
    "grt",
}


def get_spatial_con(
    database: str | Path = ":memory:",
    read_only: bool = False,
    config: dict[str, str | bool | int | float | list[str]] = {},
) -> DuckDBPyConnection:
    con = duckdb.connect(database=database, read_only=read_only, config=config)
    con.install_extension("spatial")
    con.load_extension("spatial")
    return con


def is_traj_file(file: Path):
    if file.suffix not in {".parquet", ".pq"}:
        return False

    file_columns = duckdb.query(
        "select name from parquet_schema(?) where name != 'duckdb_schema';",
        params=[file.as_posix()],
    ).fetchall()
    file_columns = {c[0] for c in file_columns}

    if TRAJ_FILE_COLUMNS.issubset(file_columns):
        return True
    return False
