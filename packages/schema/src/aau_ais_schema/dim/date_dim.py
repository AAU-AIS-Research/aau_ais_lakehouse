import duckdb
from adbc_driver_manager.dbapi import Connection
from duckdb import ColumnExpression
from pyarrow import Table

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SmartKeyMergeStrategy


class DateIdExpander:
    def __call__(self, batch: Table, name_map: dict[str, str]) -> Table:
        smart_key = ColumnExpression(name_map["date_id"])
        q = f"""--sql
select distinct
    {smart_key},
    year(ts)::usmallint              as year_no,
    month(ts)::utinyint              as month_no,
    day(ts)::utinyint                as day_no,
    weekofyear(ts)::utinyint         as week_no,
    isodow(ts)::utinyint             as weekday_no,
    quarter(ts)::utinyint            as quarter_no,
    strftime(ts, '%Y-%m-%d')         as iso_date,
    strftime(ts, '%B')               as month_name
from batch, lateral (select strptime({smart_key}::text, '%Y%m%d')) as t(ts)
"""
        with duckdb.connect() as con:
            return con.query(q).to_arrow_table()


class DateDim(Dimension):
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "date_dim",
        pre_processors: list[Processor] = [],
    ) -> None:
        merge_strategy = SmartKeyMergeStrategy()
        columns = [
            "date_id",
            "year_no",
            "month_no",
            "day_no",
            "week_no",
            "weekday_no",
            "quarter_no",
            "iso_date",
            "month_name",
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
