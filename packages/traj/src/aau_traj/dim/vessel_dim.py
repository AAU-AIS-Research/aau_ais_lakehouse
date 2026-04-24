import time

from adbc_driver_flightsql.dbapi import Connection
from loguru import logger
from pyarrow import RecordBatchReader, Table
from pyarrow.compute import field


def __ingest(con: Connection, data: RecordBatchReader) -> Table:
    tmp_tbl = "src_vessel_dim"
    with con.cursor() as curs:
        curs.execute(f"drop table if exists {tmp_tbl};")
        curs.adbc_ingest(tmp_tbl, data, mode="replace", temporary=True)

        q = f"""--sql
-- Add helper columns
alter table {tmp_tbl}
    add column if not exists vessel_id uinteger;
alter table {tmp_tbl}
    add column if not exists is_new boolean default true;

-- Update existing records
update {tmp_tbl} as src
    set vessel_id = dst.vessel_id,
        is_new = false
from dim.vessel_dim as dst
where src.mmsi = dst.mmsi
    and src.imo is not distinct from dst.imo;

-- Insert new rows
insert into dim.vessel_dim by name (
    select distinct
            mmsi,
            imo,
            mid,
            radio_service_type,
            is_valid_mmsi,
            is_valid_imo,
            in_eu_mrv_db
    from {tmp_tbl}
    where is_new
);

-- Final update to sync the newly generated IDs back to the temporary table
update {tmp_tbl} as src
    set vessel_id = dst.vessel_id
from dim.vessel_dim as dst
where src.mmsi = dst.mmsi
    and src.imo is not distinct from dst.imo
    and src.is_new = true;
"""
        curs.execute(q)

        q = f"select vessel_id, mmsi, imo, is_new from {tmp_tbl};"
        return curs.execute(q).fetch_arrow_table()


def load(con: Connection, src: RecordBatchReader) -> Table:
    logger.info("Loading vessel_dim...")
    s = time.perf_counter()
    result = __ingest(con, src)
    logger.info(
        "Inserted {:,} row(s) into vessel_dim in {:,.2f}s",
        len(result.filter(field("is_new"))),
        time.perf_counter() - s,
    )
    return result
