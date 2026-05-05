import time
from typing import Any, Callable

import duckdb
from adbc_driver_manager.dbapi import Connection
from duckdb import ColumnExpression
from loguru import logger
from pyarrow import Table

Processor = Callable[[Table, dict[str, str]], Table]


class DateIdExpander:
    def __call__(self, batch: Table, keys: dict[str, str]) -> Any:
        date_id_col = ColumnExpression(keys["date_id"])
        q = f"""--sql
select distinct
    {date_id_col}                    as date_id,
    year(ts)::usmallint              as year_no,
    month(ts)::utinyint              as month_no,
    day(ts)::utinyint                as day_no,
    weekofyear(ts)::utinyint         as week_no,
    isodow(ts)::utinyint             as weekday_no,
    quarter(ts)::utinyint            as quarter_no,
    strftime(ts, '%Y-%m-%d')         as iso_date,
    strftime(ts, '%B')               as month_name
from batch, lateral (select strptime({date_id_col}::text, '%Y%m%d')) as t(ts)
"""
        with duckdb.connect() as con:
            return con.query(q).to_arrow_table()


class DateDim:
    def __init__(
        self,
        con: Connection,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "date_dim",
    ) -> None:
        self.__con = con
        self.__catalog = catalog_name
        self.__schema = schema_name
        self.__table = table_name
        self.__staging_table = "staging_" + table_name

    @property
    def catalog_name(self) -> str:
        return self.__catalog

    @property
    def schema_name(self) -> str:
        return self.__schema

    @property
    def table_name(self) -> str:
        return self.__table

    @property
    def staging_table_name(self) -> str:
        return self.__staging_table

    @property
    def table(self):
        return f'"{self.catalog_name}"."{self.schema_name}"."{self.table_name}"'

    def count(self) -> int:
        with self.__con.cursor() as cursor:
            q = f"select count(*) from {self.table}"
            return cursor.execute(q).fetchall()[0][0]

    def __pre_process(
        self, batch: Table, pre_processors: list[Processor], keys: dict[str, str]
    ) -> Table:
        for process in pre_processors:
            batch = process(batch, keys)
        return batch

    def __stage(self, batch: Table):
        with self.__con.cursor() as cursor:
            cursor.adbc_ingest(
                table_name=self.staging_table_name,
                data=batch,
                mode="replace",
                temporary=True,
            )

    def __load(self):
        q = f"""--sql
insert or ignore into {self.table} by name (
    select
        date_id,
        year_no,
        month_no,
        day_no,
        week_no,
        weekday_no,
        quarter_no,
        iso_date,
        month_name
    from {self.staging_table_name}
);
"""
        with self.__con.cursor() as cursor:
            cursor.execute(q)

    def load(
        self,
        batch: Table,
        pre_processors: list[Processor] = [],
        keys: dict[str, str] = {"date_id": "date_id"},
    ) -> Table:
        logger.info("Loading date_dim...")
        start_cnt = self.count()
        s = time.perf_counter()

        data = self.__pre_process(batch, pre_processors, keys)
        self.__stage(data)
        self.__load()

        end_cnt = self.count()

        logger.info(
            "Inserted {:,} row(s) into {} in {:,.2f}s",
            end_cnt - start_cnt,
            self.table,
            time.perf_counter() - s,
        )

        return batch
