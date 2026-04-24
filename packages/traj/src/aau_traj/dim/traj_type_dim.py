import time

import duckdb
from adbc_driver_gizmosql.dbapi import Connection
from loguru import logger
from pyarrow import RecordBatchReader, Table
from pyarrow.compute import field


def __ingest(con: Connection, data: RecordBatchReader) -> Table:
    tmp_tbl = "src_traj_type_dim"
    with con.cursor() as curs:
        curs.execute(f"drop table if exists {tmp_tbl};")
        curs.adbc_ingest(tmp_tbl, data, mode="replace", temporary=True)

        q = f"""--sql
-- Add new columns
alter table {tmp_tbl}
    add column if not exists traj_type_id usmallint;
alter table {tmp_tbl}
    add column if not exists is_new boolean default true;

-- Update temporary src table
update {tmp_tbl} as src
    set traj_type_id = dst.traj_type_id,
        is_new = false
from dim.traj_type_dim as dst
where src.traj_type = dst.traj_type;

-- Insert new rows into dim.traj_type_dim
insert into dim.traj_type_dim by name (
    select traj_type
    from {tmp_tbl}
    where is_new
);

-- Fetch IDs for new rows
update {tmp_tbl} as src
    set traj_type_id = dst.traj_type_id
from dim.traj_type_dim as dst
where src.traj_type = dst.traj_type
    and src.traj_type_id is null;
"""
        curs.executescript(q)

        q = f"""--sql
select
    traj_type_id,
    traj_type,
    is_new
from {tmp_tbl};
"""
        return curs.execute(q).fetch_arrow_table()


def load(dst_con: Connection, data: RecordBatchReader) -> Table:
    logger.info("Loading traj_type_dim...")

    s = time.perf_counter()

    data = (
        duckdb.from_arrow(data)
        .select("* replace (coalesce(traj_type, 'unknown') as traj_type)")
        .fetch_arrow_reader()
    )  # Ensure no null traj_type values
    result = __ingest(dst_con, data)

    logger.info(
        "Inserted {:,} row(s) into traj_type_dim in {:,.2f}s",
        len(result.filter(field("is_new"))),
        time.perf_counter() - s,
    )
    return result
