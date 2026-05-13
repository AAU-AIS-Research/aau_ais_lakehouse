import time
from abc import ABC
from collections.abc import Sequence
from typing import Callable

import duckdb
from aau_ais_core import duckdb_utils
from adbc_driver_manager.dbapi import Connection
from jinja2 import Template
from loguru import logger
from pyarrow import Table

from aau_ais_schema.merge_strategies import MergeStrategy

Processor = Callable[[Table, dict[str, str]], Table]


class Dimension(ABC):
    def __init__(
        self,
        con: Connection,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        columns: Sequence[str],
        merge_strategy: MergeStrategy,
        pre_processors: list[Processor] = [],
        max_ingest_chunk_size: int | None = None,
    ) -> None:
        self._con = con
        self.__catalog = catalog_name
        self.__schema = schema_name
        self.__table = table_name
        self.__staging_table = "staging_" + table_name
        self.__columns = columns
        self.__merge_strategy = merge_strategy
        self.__pre_processors = pre_processors
        self.__max_ingest_chunk_size = max_ingest_chunk_size

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
    def columns(self) -> Sequence[str]:
        return self.__columns

    @property
    def max_ingest_chunk_size(self) -> int | None:
        return self.__max_ingest_chunk_size

    def count(self) -> int:
        with self._con.cursor() as cursor:
            q = f"select count(*) from {self.table}"
            return cursor.execute(q).fetchall()[0][0]

    def __apply_pre_processes(self, batch: Table, name_map: dict[str, str]) -> Table:
        for process in self.__pre_processors:
            batch = process(batch, name_map)
        return batch

    def __resolve_name_map(self, name_map: dict[str, str]):
        """
        Construct the final name mapping by starting with default column mappings
        and overriding them with the provided name_map.

        Args:
            name_map: A dictionary of name mappings to use. Overrides default
                      self.columns mappings if keys overlap.

        Returns:
            A dictionary containing the final name mapping.
        """
        rv = {c: c for c in self.columns}
        rv.update(name_map)
        return rv

    def __trim(self, batch: Table, name_map: dict[str, str]):
        template_str = """--sql
select distinct
    {%- for column, column_map in columns.items() %}
    {{column_map}}{% if column != column_map %} as {{column}}{% endif %},
    {%- endfor %}
from batch;
"""
        q = Template(template_str).render(
            columns={k: name_map[k] for k in self.columns},
        )
        with duckdb.connect().begin() as con:
            return con.query(q).fetch_arrow_table()

    def __cast_geometry_columns(self, con: Connection, src_tbl: str) -> None:
        """This is a workaround accommodating an issue where geometry data is inserted as Blobs"""
        geom_columns = [
            col
            for col in duckdb_utils.fetch_columns(con, self.table)
            if col.type.lower().startswith("geometry")
        ]
        if len(geom_columns) == 0:
            return

        q = ""
        for col in geom_columns:
            q += f"alter table {src_tbl} alter {col.name} set data type geometry using ST_GeomFromWKB({col.name});\n"

        with con.cursor() as curs:
            curs.execute(q)

    def __stage(self, batch: Table):
        data = batch
        if self.max_ingest_chunk_size:
            data = batch.to_reader(self.max_ingest_chunk_size)
        with self._con.cursor() as cursor:
            cursor.adbc_ingest(
                table_name=self.staging_table_name,
                data=data,
                mode="replace",
                temporary=True,
            )
        # The below should be a temporary fix to GizmoSQL adbc driver ingesting geometry columns as blob
        self.__cast_geometry_columns(self._con, self.staging_table_name)

    def load(
        self,
        batch: Table,
        name_map: dict[str, str] = {},
    ) -> Table:
        printable_tbl_name = self.table.replace('"', "")
        logger.info("Loading {}...", printable_tbl_name)
        start_cnt = self.count()
        s = time.perf_counter()

        name_map = self.__resolve_name_map(name_map)
        data = self.__apply_pre_processes(batch, name_map)
        data = self.__trim(data, name_map)
        self.__stage(data)
        batch = self.__merge_strategy(
            self._con,
            batch,
            self.staging_table_name,
            self.table,
            name_map,
        )

        end_cnt = self.count()

        logger.info(
            "Inserted {:,} row(s) into {} in {:,.2f}s",
            end_cnt - start_cnt,
            printable_tbl_name,
            time.perf_counter() - s,
        )

        return batch
