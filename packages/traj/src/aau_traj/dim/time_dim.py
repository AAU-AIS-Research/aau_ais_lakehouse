import time

import duckdb
from adbc_driver_flightsql.dbapi import Connection
from duckdb import ColumnExpression, Expression
from loguru import logger
from pyarrow import RecordBatchReader, Table
from pyarrow.compute import field


def __transform(src: RecordBatchReader) -> RecordBatchReader:
    q = """--sql
create or replace macro minutes_since_midnight(ts) as
    hour(ts::timestamp) * 60 + minute(ts::timestamp);

create or replace macro time_id_to_timestamp(time_id) as
    strptime(lpad(time_id::text, 6, '0'), '%H%M%S');

select distinct
        time_id,
        hour(ts)::utinyint                                          as hour_no,
        minute(ts)::utinyint                                        as minute_no,
        second(ts)::utinyint                                        as second_no,
        cast(floor(minutes_since_midnight(ts) / 15) as utinyint)    as fifteen_min_no,
        cast(floor(minutes_since_midnight(ts) / 5) as usmallint)    as five_min_no
from src, lateral (select time_id_to_timestamp(time_id)) as t(ts)
"""
    with duckdb.connect() as con:
        return con.query(q).to_arrow_reader()


def __stage(dst_con: Connection, data: RecordBatchReader, staging_tbl: Expression):

    q = f"""--sql
alter table {staging_tbl} add column is_new boolean default true;

update {staging_tbl} as src
    set is_new = false
from dim.time_dim as dst
where src.time_id = dst.time_id;
"""
    with dst_con.cursor() as curs:
        curs.adbc_ingest(staging_tbl.get_name(), data, mode="replace", temporary=True)
        curs.execute(q)


def __load(dst_con: Connection, staging_tbl: Expression) -> Table:
    q = f"""--sql
insert into dim.time_dim by name (
    select * exclude(is_new)
    from {staging_tbl}
    where is_new
);
"""
    with dst_con.cursor() as curs:
        curs.execute(q)
        return curs.execute(
            f"select time_id, is_new from {staging_tbl};"
        ).fetch_arrow_table()


def load(dst_con: Connection, data: RecordBatchReader) -> Table:
    logger.info("Loading time_dim...")
    s = time.perf_counter()

    staging_tbl = ColumnExpression("staging_time_dim")

    data = __transform(data)
    __stage(dst_con, data, staging_tbl)
    result = __load(dst_con, staging_tbl)
    logger.info(
        "Inserted {:,} row(s) into time_dim in {:,.2f}s",
        len(result.filter(field("is_new"))),
        time.perf_counter() - s,
    )

    return result


# def __add_time_id(con: Connection):
#     q = """--sql
# alter table src add column time_id uinteger;
# update src set time_id = strftime(timestamp, '%-H%M%S')::uinteger;
# """
#     with con.cursor() as curs:
#         curs.execute(q).fetchall()


# def __load(con: Connection) -> int:
#     q = """--sql
# with distinct_src_times as (
#     select distinct
#         time_id,
#         hour(timestamp)::utinyint                                               as hour_no,
#         minute(timestamp)::utinyint                                             as minute_no,
#         second(timestamp)::utinyint                                             as second_no,
#         cast(round(minutes_since_midnight(timestamp) / 15, 0) + 1 as utinyint)  as fifteen_min_no,
#         cast(round(minutes_since_midnight(timestamp) / 5, 0) + 1 as usmallint)  as five_min_no
#     from src
# ), missing as (
#     select *
#     from distinct_src_times
#         anti join dim.time_dim using (time_id)
# )
# insert into dim.time_dim from missing;
# """
#     with con.cursor() as curs:
#         return curs.execute(q).fetchall()[0][0]


# def load(con: Connection) -> int:
#     logger.info("Adding time_id column to src...")
#     __add_time_id(con)

#     logger.info("Loading time_dim...")
#     s = time.perf_counter()
#     inserted_rows = __load(con)
#     logger.info(
#         "Inserted {:,} row(s) into time_dim in {:,.2f}s",
#         inserted_rows,
#         time.perf_counter() - s,
#     )

#     return inserted_rows
