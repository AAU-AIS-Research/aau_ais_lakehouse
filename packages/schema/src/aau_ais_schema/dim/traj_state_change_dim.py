import time

import duckdb
from adbc_driver_manager.dbapi import Connection
from loguru import logger
from pyarrow import RecordBatchReader, Table
from pyarrow.compute import field


def __ingest(con: Connection, data: RecordBatchReader) -> Table:
    tmp_tbl = "src_traj_state_change_dim"
    with con.cursor() as curs:
        curs.execute(f"drop table if exists {tmp_tbl};")
        curs.adbc_ingest(tmp_tbl, data, mode="replace", temporary=True)

        q = f"""--sql
-- Add new columns
alter table {tmp_tbl}
    add column if not exists state_change_id usmallint;
alter table {tmp_tbl}
    add column if not exists is_new boolean default true;

-- Update temporary src table
update {tmp_tbl} as src
    set state_change_id = dst.state_change_id,
        is_new = false
from dim.traj_state_change_dim as dst
where src.state_change = dst.state_change;

-- Insert new rows into dim.traj_state_change_dim
insert into dim.traj_state_change_dim by name (
    select state_change
    from {tmp_tbl}
    where is_new
);

-- Fetch IDs for new rows
update {tmp_tbl} as src
    set state_change_id = dst.state_change_id
from dim.traj_state_change_dim as dst
where src.state_change = dst.state_change
    and src.is_new;
"""
        curs.execute(q)

        # Fetch the updated src table with state_change_id
        q = f"""--sql
select
    state_change_id,
    state_change,
    is_new
from {tmp_tbl};
"""
        return curs.execute(q).fetch_arrow_table()


def load(dst_con: Connection, data: RecordBatchReader) -> Table:
    logger.info("Loading traj_state_change_dim...")

    data = (
        duckdb.from_arrow(data)
        .select("coalesce(state_change, 'unknown') as state_change")
        .distinct()
        .fetch_arrow_reader()
    )

    s = time.perf_counter()
    result = __ingest(dst_con, data)

    logger.info(
        "Inserted {:,} row(s) into traj_state_change_dim in {:,.2f}s",
        len(result.filter(field("is_new"))),
        time.perf_counter() - s,
    )
    return result
