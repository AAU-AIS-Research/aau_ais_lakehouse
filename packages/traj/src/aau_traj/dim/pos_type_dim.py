import time

from adbc_driver_flightsql.dbapi import Connection
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
    add column if not exists pos_type_id usmallint;
alter table {tmp_tbl}
    add column if not exists is_new boolean default true;

-- Update temporary src table
update {tmp_tbl} as src
    set pos_type_id = dst.pos_type_id,
        is_new = false
from dim.pos_type_dim as dst
where src.pos_type = dst.pos_type;

-- Insert new rows into dim.pos_type_dim
insert into dim.pos_type_dim by name (
    select pos_type
    from {tmp_tbl}
    where is_new
);

update {tmp_tbl} as src
    set pos_type_id = dst.pos_type_id
from dim.pos_type_dim as dst
where src.is_new
    and src.pos_type = dst.pos_type;
"""
        curs.execute(q)

        # Fetch the updated src table with pos_type_id
        q = f"""--sql
select
    pos_type_id,
    pos_type,
    is_new
from {tmp_tbl};        
"""
        return curs.execute(q).fetch_arrow_table()


def load(con: Connection, src: RecordBatchReader) -> Table:
    logger.info("Loading pos_type_dim...")
    s = time.perf_counter()
    result = __ingest(con, src)
    logger.info(
        "Inserted {:,} row(s) into pos_type_dim in {:,.2f}s",
        len(result.filter(field("is_new"))),
        time.perf_counter() - s,
    )

    return result
