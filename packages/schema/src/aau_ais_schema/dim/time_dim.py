from typing import Any

import duckdb
from aau_ais_core import duckdb_macros
from adbc_driver_manager.dbapi import Connection
from duckdb import ColumnExpression
from pyarrow import Table

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SmartKeyMergeStrategy


class TimeIdExpander:
    def __call__(self, batch: Table, name_map: dict[str, str]) -> Any:
        smart_key = ColumnExpression(name_map["time_id"])
        q = f"""--sql
select distinct
        {smart_key},
        hour(ts)::utinyint                                          as hour_no,
        minute(ts)::utinyint                                        as minute_no,
        second(ts)::utinyint                                        as second_no,
        cast(floor(minutes_since_midnight(ts) / 15) as utinyint)    as fifteen_min_no,
        cast(floor(minutes_since_midnight(ts) / 5) as usmallint)    as five_min_no
from batch, lateral (select time_id_to_timestamp({smart_key})) as t(ts)
"""
        with duckdb.connect() as con:
            duckdb_macros.create_time_id_to_timestamp(con)
            duckdb_macros.create_minutes_since_midnight(con)
            return con.query(q).to_arrow_table()


class TimeDim(Dimension):
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "time_dim",
        pre_processors: list[Processor] = [],
    ) -> None:
        merge_strategy = SmartKeyMergeStrategy()
        columns = [
            "time_id",
            "hour_no",
            "minute_no",
            "second_no",
            "fifteen_min_no",
            "five_min_no",
        ]
        super().__init__(
            con,
            catalog_name,
            schema_name,
            table_name,
            columns,
            merge_strategy,
            pre_processors,
        )
