import time

from adbc_driver_manager.dbapi import Connection
from loguru import logger
from pyarrow import RecordBatchReader, Table
from pyarrow.compute import field


def __ingest(con: Connection, data: RecordBatchReader) -> Table:
    tmp_tbl = "src_nav_status_dim"
    with con.cursor() as curs:
        curs.execute(f"drop table if exists {tmp_tbl};")
        curs.adbc_ingest(tmp_tbl, data, mode="replace", temporary=True)

        q = f"""--sql
-- Add new columns
alter table {tmp_tbl}
    add column if not exists nav_status_id usmallint;
alter table {tmp_tbl}
    add column if not exists is_new boolean default true;

-- Update temporary src table
update {tmp_tbl} as src
    set nav_status_id = dst.nav_status_id,
        is_new = false
from dim.nav_status_dim as dst
where src.nav_status = dst.nav_status;

-- Insert new rows into dim.nav_status_dim
insert into dim.nav_status_dim by name (
    select nav_status
    from {tmp_tbl}
    where is_new
);

update {tmp_tbl} as src
    set nav_status_id = dst.nav_status_id
from dim.nav_status_dim as dst
where is_new
    and src.nav_status = dst.nav_status;
"""
        curs.execute(q)

        # Fetch the updated src table with nav_status_id
        q = f"""--sql
select
    nav_status_id,
    nav_status,
    is_new
from {tmp_tbl};        
"""
        return curs.execute(q).fetch_arrow_table()


def load(con: Connection, src: RecordBatchReader) -> Table:
    logger.info("Loading nav_status_dim...")
    s = time.perf_counter()
    result = __ingest(con, src)
    logger.info(
        "Inserted {:,} row(s) into nav_status_dim in {:,.2f}s",
        len(result.filter(field("is_new"))),
        time.perf_counter() - s,
    )

    return result
