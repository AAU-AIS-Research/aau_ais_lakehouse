import time

from adbc_driver_manager.dbapi import Connection
from loguru import logger
from pyarrow import RecordBatchReader, Table
from pyarrow.compute import field


def __ingest(con: Connection, data: RecordBatchReader) -> Table:
    tmp_tbl = "src_vessel_config_dim"
    with con.cursor() as curs:
        curs.execute(f"drop table if exists {tmp_tbl};")
        curs.adbc_ingest(tmp_tbl, data, mode="replace", temporary=True)

        q = f"""--sql
-- Add new columns
alter table {tmp_tbl}
    add column if not exists vessel_config_id int;
alter table {tmp_tbl}
    add column if not exists is_new boolean default true;

-- Update temporary src table with existing IDs
update {tmp_tbl} as src
    set vessel_config_id = dst.vessel_config_id,
        is_new = false
from dim.vessel_config_dim as dst
where src.length is not distinct from dst.length
    and src.width is not distinct from dst.width
    and src.height is not distinct from dst.height
    and src.max_draught is not distinct from dst.max_draught
    and src.dwt is not distinct from dst.dwt
    and src.grt is not distinct from dst.grt
    and src.to_bow is not distinct from dst.to_bow
    and src.to_stern is not distinct from dst.to_stern
    and src.to_port is not distinct from dst.to_port
    and src.to_starboard is not distinct from dst.to_starboard
    and src.main_engine_kwh is not distinct from dst.main_engine_kwh
    and src.aux_engine_kwh is not distinct from dst.aux_engine_kwh;

-- Handle ID generation and insertion for new rows
insert into dim.vessel_config_dim by name (
    select * exclude (vessel_config_id, is_new)
    from {tmp_tbl}
    where is_new
);

-- Update the temporary table one last time to include the newly created IDs
update {tmp_tbl} as src
    set vessel_config_id = dst.vessel_config_id
from dim.vessel_config_dim as dst
where src.is_new
    and src.length is not distinct from dst.length
    and src.width is not distinct from dst.width
    and src.height is not distinct from dst.height
    and src.max_draught is not distinct from dst.max_draught
    and src.dwt is not distinct from dst.dwt
    and src.grt is not distinct from dst.grt
    and src.to_bow is not distinct from dst.to_bow
    and src.to_stern is not distinct from dst.to_stern
    and src.to_port is not distinct from dst.to_port
    and src.to_starboard is not distinct from dst.to_starboard
    and src.main_engine_kwh is not distinct from dst.main_engine_kwh
    and src.aux_engine_kwh is not distinct from dst.aux_engine_kwh;
"""
        curs.execute(q)

        # Fetch the updated src table with vessel_config_id
        q = f"select * from {tmp_tbl};"
        return curs.execute(q).fetch_arrow_table()


def load(con: Connection, src: RecordBatchReader) -> Table:
    logger.info("Loading vessel_config_dim...")
    s = time.perf_counter()
    result = __ingest(con, src)

    # Calculate how many were new based on the initial state before the final update
    # Note: in the final result, is_new is now false for all.
    # To track inserted rows accurately, we could count the diff in the dim table.
    logger.info(
        "Inserted {:,} row(s) into vessel_config_dim in {:,.2f}s",
        len(result.filter(field("is_new"))),
        time.perf_counter() - s,
    )

    return result
