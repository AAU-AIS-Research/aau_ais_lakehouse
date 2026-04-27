import time

from adbc_driver_manager.dbapi import Connection
from loguru import logger
from pyarrow import RecordBatchReader, Table
from pyarrow.compute import field


def __ingest(con: Connection, data: RecordBatchReader) -> Table:
    tmp_tbl = "src_pos_type_dim"
    with con.cursor() as curs:
        curs.adbc_ingest(tmp_tbl, data, mode="replace", temporary=True)
        q = f"""--sql
-- Add new columns
alter table {tmp_tbl}
    add column if not exists destination_id uinteger;
alter table {tmp_tbl}
    add column if not exists is_new boolean default true;

-- Update temporary src table
update {tmp_tbl} as src
    set destination_id = dst.destination_id,
        is_new = false
from dim.destination_dim as dst
where src.org_msg = dst.org_msg;

-- Insert new rows into dim.pos_type_dim
insert into dim.destination_dim by name (
    select org_msg
    from {tmp_tbl}
    where is_new
);

update {tmp_tbl} as src
    set destination_id = dst.destination_id
from dim.destination_dim as dst
where src.is_new
    and src.org_msg = dst.org_msg;
"""
        curs.execute(q)

        # Fetch the updated src table with destination_id
        q = f"""--sql
select
    destination_id,
    org_msg,
    is_new
from {tmp_tbl};        
"""
        return curs.execute(q).fetch_arrow_table()


def load(con: Connection, src: RecordBatchReader) -> Table:
    logger.info("Loading destination_dim...")
    s = time.perf_counter()
    result = __ingest(con, src)
    logger.info(
        "Inserted {:,} row(s) into destination_dim in {:,.2f}s",
        len(result.filter(field("is_new"))),
        time.perf_counter() - s,
    )

    return result
