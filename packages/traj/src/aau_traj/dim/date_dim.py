import time

import duckdb
from adbc_driver_flightsql.dbapi import Connection
from duckdb import ColumnExpression, Expression
from loguru import logger
from pyarrow import RecordBatchReader, Table
from pyarrow.compute import field


def __transform(src: RecordBatchReader) -> RecordBatchReader:
    q = """--sql
select distinct
    date_id,
    year(ts)::usmallint              as year_no,
    month(ts)::utinyint              as month_no,
    day(ts)::utinyint                as day_no,
    weekofyear(ts)::utinyint         as week_no,
    isodow(ts)::utinyint             as weekday_no,
    quarter(ts)::utinyint            as quarter_no,
    strftime(ts, '%Y-%m-%d')         as iso_date,
    strftime(ts, '%B')               as month_name
from src, lateral (select strptime(date_id::text, '%Y%m%d')) as t(ts)
"""
    with duckdb.connect() as con:
        return con.query(q).to_arrow_reader()


def __stage(dst_con: Connection, data: RecordBatchReader, staging_tbl: Expression):

    q = f"""--sql
alter table {staging_tbl} add column is_new boolean default true;

update {staging_tbl} as src
    set is_new = false
from dim.date_dim as dst
where src.date_id = dst.date_id;
"""
    with dst_con.cursor() as curs:
        curs.adbc_ingest(staging_tbl.get_name(), data, mode="replace", temporary=True)
        curs.execute(q)


def __load(dst_con: Connection, staging_tbl: Expression) -> Table:
    q = f"""--sql
insert into dim.date_dim by name (
    select * exclude(is_new)
    from {staging_tbl}
    where is_new
);
"""
    with dst_con.cursor() as curs:
        curs.execute(q)
        return curs.execute(
            f"select date_id, is_new from {staging_tbl};"
        ).fetch_arrow_table()


def load(dst_con: Connection, data: RecordBatchReader) -> Table:
    logger.info("Loading date_dim...")
    s = time.perf_counter()

    staging_tbl = ColumnExpression("staging_date_dim")

    data = __transform(data)
    __stage(dst_con, data, staging_tbl)
    result = __load(dst_con, staging_tbl)
    logger.info(
        "Inserted {:,} row(s) into date_dim in {:,.2f}s",
        len(result.filter(field("is_new"))),
        time.perf_counter() - s,
    )

    return result
