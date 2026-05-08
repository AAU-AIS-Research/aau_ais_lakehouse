import time

from adbc_driver_manager.dbapi import Connection
from loguru import logger
from pyarrow import RecordBatchReader, Table
from pyarrow.compute import field

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SurrogateKeyMergeStrategy


class CallSignDim(Dimension):
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "call_sign_dim",
        pre_processors: list[Processor] = [],
    ):
        surrogate = "call_sign_id"
        keys = ["call_sign"]

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
