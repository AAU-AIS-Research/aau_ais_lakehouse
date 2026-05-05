import time
from typing import Any, Callable

import duckdb
from adbc_driver_manager.dbapi import Connection
from duckdb import ColumnExpression
from loguru import logger
from pyarrow import Table

Processor = Callable[[Table, str], Table]


class DateIdExpander:
    def __init__(self, date_id: str = "date_id"):
        self.__date_id = date_id

    def __call__(self, batch: Table, row_id) -> Any:
        row_id_col = ColumnExpression(row_id)
        date_id_col = ColumnExpression(self.date_id)
        q = f"""--sql
    select distinct
        {row_id_col},
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

    @property
    def date_id(self) -> str:
        return self.__date_id


class TimestampExpander:
    def __init__(self, timestamp: str = "timestamp"):
        self.__timestamp = timestamp

    @property
    def timestamp(self) -> str:
        return self.__timestamp

    def __call__(self, batch: Table, row_id) -> Any:
        row_id_col = ColumnExpression(row_id)
        timestamp_col = ColumnExpression(self.timestamp)
        q = f"""--sql
    select distinct
        {row_id_col},
        strftime({timestamp_col}, '%Y%m%d')::uinteger   as date_id,
        year({timestamp_col})                           as year_no,
        month({timestamp_col})                          as month_no,
        day({timestamp_col})                            as day_no,
        weekofyear({timestamp_col})                     as week_no,
        isodow({timestamp_col})                         as weekday_no,
        quarter({timestamp_col})                        as quarter_no,
        strftime({timestamp_col}, '%Y-%m-%d')           as iso_date,
        strftime({timestamp_col}, '%B')                 as month_name
    from batch;
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
        row_id: str = "row_id",
    ) -> None:
        self.__con = con
        self.__catalog = catalog_name
        self.__schema = schema_name
        self.__table = table_name
        self.__staging_table = "staging_" + table_name
        self.__row_id = row_id

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

    @property
    def row_id(self) -> str:
        return self.__row_id

    def count(self) -> int:
        with self.__con.cursor() as cursor:
            q = f"select count(*) from {self.table}"
            return cursor.execute(q).fetchall()[0][0]

    def __pre_process(self, batch: Table, pre_processors: list[Processor]) -> Table:
        for process in pre_processors:
            batch = process(batch, self.row_id)
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

    def __join(self, batch: Table) -> Table:
        row_id_col = ColumnExpression(self.row_id)
        with self.__con.cursor() as cursor:
            q = f"select {row_id_col}, date_id from {self.staging_table_name};"
            inserted = cursor.execute(q).fetch_arrow_table()
            return batch.join(inserted, keys=self.row_id, join_type="inner")

    def load(self, batch: Table, pre_processors: list[Processor] = []) -> Table:
        logger.info("Loading date_dim...")
        start_cnt = self.count()
        s = time.perf_counter()

        data = self.__pre_process(batch, pre_processors)
        self.__stage(data)
        self.__load()

        end_cnt = self.count()

        logger.info(
            "Inserted {:,} row(s) into {} in {:,.2f}s",
            end_cnt - start_cnt,
            self.table,
            time.perf_counter() - s,
        )

        return self.__join(batch)
