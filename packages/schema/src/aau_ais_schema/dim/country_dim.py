import time

from adbc_driver_manager.dbapi import Connection
from loguru import logger
from pyarrow import RecordBatchReader, Table
from pyarrow.compute import field


def __ingest(con: Connection, data: RecordBatchReader) -> Table:
    tmp_tbl = "src_country_dim"
    with con.cursor() as curs:
        curs.execute(f"drop table if exists {tmp_tbl};")
        curs.adbc_ingest(tmp_tbl, data, mode="replace", temporary=True)

        q = f"""--sql
-- Add new columns
alter table {tmp_tbl}
    add column if not exists is_new boolean default true;

-- Update temporary src table
update {tmp_tbl} as src
    set is_new = false
from dim.country_dim as dst
where src.alpha2 = dst.alpha2;

-- Insert new rows into dim.country_dim
insert into dim.country_dim by name (
    select
        alpha2,
        alpha3,
        country_name,
        country_code,
        region,
        sub_region,
        intermediate_region,
        region_code,
        sub_region_code,
        intermediate_region_code
    from {tmp_tbl}
    where is_new
);
"""
        curs.execute(q)

        # Fetch the updated src table with country_id
        return curs.execute(
            f"select alpha2, is_new from {tmp_tbl};"
        ).fetch_arrow_table()


def load(con: Connection, src: RecordBatchReader) -> Table:
    logger.info("Loading country_dim...")
    s = time.perf_counter()
    result = __ingest(con, src)
    logger.info(
        "Inserted {:,} row(s) into country_dim in {:,.2f}s",
        len(result.filter(field("is_new"))),
        time.perf_counter() - s,
    )

    return result


# import time

# from adbc_driver_flightsql.dbapi import Connection
# from loguru import logger


# def __ensure_no_data_row(con: Connection):
#     q = """--sql
# insert or ignore into dim.country_dim by position
#     values ('??', '???', 'unknown', 0, 0, 0, 0, NULL, NULL, NULL);
# """
#     with con.cursor() as curs:
#         curs.execute(q).fetchall()


# def __load(con: Connection) -> int:
#     q = """--sql
# with distinct_src_countries as (
#     select distinct
#         alpha2,
#         alpha3,
#         country_name,
#         country_code,
#         region,
#         sub_region,
#         intermediate_region,
#         region_code,
#         sub_region_code,
#         intermediate_region_code
#     from src
#     where country_name is not null
# ), missing as (
#     select *
#     from distinct_src_countries
#         anti join dim.country_dim using (country_name)
# )
# insert into dim.country_dim by name from missing;
# """

#     with con.cursor() as curs:
#         return curs.execute(q).fetchall()[0][0]


# def load(con: Connection) -> int:
#     __ensure_no_data_row(con)
#     logger.info("Loading country_dim...")
#     s = time.perf_counter()
#     inserted_rows = __load(con)

#     logger.info(
#         "Inserted {:,} row(s) into country_dim in {:,.2f}s",
#         inserted_rows,
#         time.perf_counter() - s,
#     )
#     return inserted_rows
