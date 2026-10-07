import logging

import duckdb
from aau_ais_core import duckdb_macros
from duckdb import DuckDBPyConnection
from pyarrow import Table

logger = logging.getLogger(__name__)


def __create_get_gap_explanation(con: DuckDBPyConnection) -> None:
    q = """
create or replace macro get_gap_explanation(state_change, spatial_gap, temporal_gap, ts_1, ts_2) as
    case
        when date(ts_1) != date(ts_2) and temporal_gap <= 600 and spatial_gap <= 4000 then 'midnight'
        when state_change is not null then state_change
        else 'unknown'
    end
        
"""
    con.execute(q)


def __create_get_gap_type(con: DuckDBPyConnection) -> None:
    q = """
create or replace macro get_gap_type(traj_type_1, traj_type_2) as
    case
        when traj_type_1 = 'in motion' and traj_type_2 = 'in motion' then 'tt'
        when traj_type_1 = 'in motion' and traj_type_2 = 'stationary' then 'ts'
        when traj_type_1 = 'stationary' and traj_type_2 = 'in motion' then 'st'
        when traj_type_1 = 'stationary' and traj_type_2 = 'stationary' then 'ss'
        else 'uk'
    end
        
"""
    con.execute(q)


def __setup_connection() -> DuckDBPyConnection:
    con = duckdb.connect()
    con.load_extension("spatial")
    duckdb_macros.create_temporal_id_to_timestamp(con)
    __create_get_gap_type(con)
    __create_get_gap_explanation(con)

    return con


def __store_data(con: DuckDBPyConnection, data: Table) -> None:
    q = """
    create temporary table tracks as 
        select 
            *,
            temporal_id_to_timestamp(start_date_id, start_time_id)  as start_ts,
            temporal_id_to_timestamp(end_date_id, end_time_id)      as end_ts
        from data;
    """
    con.execute(q)


def __get__distinct_mmsi(con: DuckDBPyConnection) -> list[int]:
    q = """
    select distinct mmsi
    from tracks
    order by mmsi;
    """

    return [row[0] for row in con.query(q).fetchall()]


def __generate_vessel_track_gaps(con: DuckDBPyConnection) -> Table:
    q = """
with gap as (
    select 
        mmsi,
        transponder_type,
        vessel_type,
        alpha2,
        lag(end_date_id) over w as start_date_id,
        start_date_id           as end_date_id,
        lag(end_time_id) over w as start_time_id,
        start_time_id           as end_time_id,
        lag(end_point) over w   as stat_point,
        start_point             as end_point,
        lag(end_ts) over w      as start_ts,
        start_ts                as end_ts,
        lag(type) over w        as start_type,
        type                    as end_type,
        state_change
    from tracks
    where type != 'outlier'
    window w as (partition by mmsi order by start_ts, end_ts desc)
    order by start_ts, end_ts
)
select
    (* exclude (stat_point, end_point, state_change)),
    ST_SetCRS(ST_MakeLine(stat_point, end_point), 'EPSG:4326')              as geom,
    epoch(end_ts - start_ts)                                                as seconds,
    round_even(ST_Length_Spheroid(ST_FlipCoordinates(geom)), 0)::int        as meters,
    get_gap_type(start_type, end_type)                                      as gap_type,
    get_gap_explanation(state_change, meters, seconds, start_ts, end_ts)    as state_change,
    coalesce(round(meters / nullif(seconds, 0) * 1.94384449)::int, 0)       as required_avg_speed_kt
from gap
where geom is not null;
"""
    return con.query(q).to_arrow_table()


def transform(data: Table) -> Table:
    with __setup_connection() as con:
        __store_data(con, data)
        return __generate_vessel_track_gaps(con)

        mmsi_list = __get__distinct_mmsi(con)

        for mmsi in mmsi_list:
            logger.debug("Creating gaps for MMSI: %s", mmsi)
            res_tbl = __generate_vessel_track_gaps(con, mmsi)
            q = """
            create sequence if not exists result_seq start with 1;
            create table if not exists result as 
                from res_tbl
                with no data;
            alter table if exists result add column if not exists id int;
            
            
            insert into result by name
                select nextval('result_seq') as id, *
                from res_tbl;
            """
            con.execute(q)
        return con.table("result").to_arrow_table()
