import time

from adbc_driver_flightsql.dbapi import Connection
from loguru import logger
from pyarrow import RecordBatchReader, Table
from pyarrow.compute import field


def __ingest(con: Connection, data: RecordBatchReader) -> Table:
    tmp_tbl = "src_call_sign_dim"
    with con.cursor() as curs:
        curs.adbc_ingest(tmp_tbl, data, mode="replace", temporary=True)

        q = f"""--sql
-- Add new columns
alter table {tmp_tbl}
    add column if not exists call_sign_id usmallint;
alter table {tmp_tbl}
    add column if not exists is_new boolean default true;

-- Update temporary src table
update {tmp_tbl} as src
    set call_sign_id = dst.call_sign_id,
        is_new = false
from dim.call_sign_dim as dst
where src.call_sign = dst.call_sign;

-- Insert new rows into dim.call_sign_dim
insert into dim.call_sign_dim by name (
    select call_sign
    from {tmp_tbl}
    where is_new
);

update {tmp_tbl} as src
    set call_sign_id = dst.call_sign_id
from dim.call_sign_dim as dst
where src.call_sign = dst.call_sign
    and src.is_new;
"""
        curs.execute(q)

        # Fetch the updated src table with call_sign_id
        q = f"""--sql
select
    call_sign_id,
    call_sign,
    is_new
from {tmp_tbl};        
"""
        return curs.execute(q).fetch_arrow_table()


def load(con: Connection, src: RecordBatchReader) -> Table:
    logger.info("Loading call_sign_dim...")
    s = time.perf_counter()
    result = __ingest(con, src)
    logger.info(
        "Inserted {:,} row(s) into call_sign_dim in {:,.2f}s",
        len(result.filter(field("is_new"))),
        time.perf_counter() - s,
    )

    return result
