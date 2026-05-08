import time

from adbc_driver_manager.dbapi import Connection
from loguru import logger
from pyarrow import RecordBatchReader, Table
from pyarrow.compute import field

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SurrogateKeyMergeStrategy


class VesselNameDim(Dimension):
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "vessel_name_dim",
        pre_processors: list[Processor] = [],
    ):
        surrogate = "vessel_name_id"
        keys = ["vessel_name"]

        merge_strategy = SurrogateKeyMergeStrategy(surrogate, keys)
        super().__init__(
            con,
            catalog_name,
            schema_name,
            table_name,
            columns=keys,
            merge_strategy=merge_strategy,
            pre_processors=pre_processors,
        )


def __ingest(con: Connection, data: RecordBatchReader) -> Table:
    tmp_tbl = "src_vessel_name_dim"
    with con.cursor() as curs:
        curs.execute(f"drop table if exists {tmp_tbl};")
        curs.adbc_ingest(tmp_tbl, data, mode="replace", temporary=True)

        q = f"""--sql
-- Add new columns
alter table {tmp_tbl}
    add column if not exists vessel_name_id uinteger;
alter table {tmp_tbl}
    add column if not exists is_new boolean default true;

-- Update temporary src table
update {tmp_tbl} as src
    set vessel_name_id = dst.vessel_name_id,
        is_new = false
from dim.vessel_name_dim as dst
where src.vessel_name = dst.vessel_name;

-- Insert new rows into dim.vessel_name_dim
insert into dim.vessel_name_dim by name (
    select vessel_name
    from {tmp_tbl}
    where is_new
);

update {tmp_tbl} as src
    set vessel_name_id = dst.vessel_name_id,
from dim.vessel_name_dim as dst
where src.is_new
    and src.vessel_name = dst.vessel_name;;
"""
        curs.execute(q)

        # Fetch the updated src table with vessel_name_id
        q = f"""--sql
select
    vessel_name_id,
    vessel_name,
    is_new
from {tmp_tbl};        
"""
        return curs.execute(q).fetch_arrow_table()


def load(con: Connection, src: RecordBatchReader) -> Table:
    logger.info("Loading vessel_name_dim...")
    s = time.perf_counter()
    result = __ingest(con, src)
    logger.info(
        "Inserted {:,} row(s) into vessel_name_dim in {:,.2f}s",
        len(result.filter(field("is_new"))),
        time.perf_counter() - s,
    )

    return result
