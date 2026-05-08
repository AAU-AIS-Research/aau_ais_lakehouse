from collections.abc import Sequence
from dataclasses import dataclass

import duckdb
from adbc_driver_manager.dbapi import Connection
from duckdb import ColumnExpression
from jinja2 import Template
from pyarrow import Table


class SurrogateKeyInsertStrategy:
    """
    A merge strategy that inserts data without key matching, but joins back the surrogate key using the given keys.
    As a result, keys will not be inserted into the dimension however, they are used to join the data back to the source.

    Unlike :class:`SurrogateKeyDimensionMergeStrategy` which uses a MERGE statement to match
    existing records, this strategy performs a simple INSERT (ignoring duplicates) and then
    joins back the surrogate key from the dimension table.

    This is useful when you want to ensure all new data is inserted, but still need the
    surrogate key for local processing.
    """

    @dataclass(slots=True, frozen=True)
    class ColumnInfo:
        name: str
        type: str
        null: str
        key: str
        default: str
        extra: str

    def __init__(
        self, surrogate_key: str, keys: Sequence[str], attributes: Sequence[str] = []
    ):
        self.__surrogate_key = surrogate_key
        self.__keys = keys
        self.__attributes = attributes

    @property
    def surrogate_key(self) -> str:
        return self.__surrogate_key

    @property
    def keys(self) -> Sequence[str]:
        return self.__keys

    @property
    def attributes(self) -> Sequence[str]:
        return self.__attributes

    def __cast_geometry_columns(
        self, con: Connection, src_tbl: str, columns: list[ColumnInfo]
    ) -> None:
        """This is a workaround accommodating an issue where geometry data is inserted as Blobs"""
        geom_columns = [
            col for col in columns if col.type.lower().startswith("geometry")
        ]
        if len(geom_columns) == 0:
            return

        q = ""
        for col in geom_columns:
            q += f"alter table {src_tbl} alter {col.name} set data type geometry using ST_GeomFromWKB({col.name});\n"

        with con.cursor() as curs:
            curs.execute(q)

    def __fetch_columns(self, con: Connection, dst_tbl: str):
        template_str = """--sql
select
    column_name,
    column_type,
    "null",
    "key",
    "default",
    extra
from (describe {{dst_tbl}});
"""
        q = Template(template_str).render(dst_tbl=dst_tbl)
        with con.cursor() as curs:
            res = curs.execute(q).fetchall()
            if len(res) == 0:
                raise ValueError(
                    f"{dst_tbl} does not seem to have a column named {self.surrogate_key}"
                )
            return [self.ColumnInfo(*columns) for columns in res]

    def __merge(
        self, con: Connection, src_tbl: str, dst_tbl: str, columns: list[ColumnInfo]
    ) -> None:
        template_str = """--sql
-- Add the surrogate key to src table
alter table {{src_tbl}} add column {{surrogate.name}} {{surrogate.type}} default {{surrogate.default}};

-- Insert data into destination table
insert into {{dst_tbl}} by name (
    select
        {{surrogate.name}},
        {{attributes | join(',\n')}}
    from {{src_tbl}}
);
"""
        surrogate_col = next(col for col in columns if col.name == self.surrogate_key)
        q = Template(template_str).render(
            src_tbl=src_tbl,
            dst_tbl=dst_tbl,
            keys=self.keys,
            attributes=self.attributes,
            surrogate=surrogate_col,
        )

        with con.cursor() as curs:
            curs.execute(q)

    def __back_join(
        self, con: Connection, batch: Table, src_tbl: str, name_map: dict[str, str]
    ) -> Table:
        template_str = """--sql
select
    {{surrogate}},
    {{keys | join(',\n')}}
from {{src_tbl}};
"""
        q = Template(template_str).render(
            src_tbl=src_tbl,
            keys=self.keys,
            attributes=self.attributes,
            surrogate=self.surrogate_key,
        )
        with con.cursor() as curs, duckdb.connect().begin() as local_con:
            staging = local_con.from_arrow(curs.execute(q).fetch_arrow_table())
            src = local_con.from_arrow(batch)
            conditions = [
                f"{staging.alias}.{key} = {src.alias}.{name_map[key]}"
                for key in self.keys
            ]

            return (
                src.join(staging, "and ".join(conditions), "inner")
                .select(f"{src.alias}.*, {self.surrogate_key}")
                .to_arrow_table()
            )

    def __call__(
        self,
        con: Connection,
        batch: Table,
        src_tbl: str,
        dst_tbl: str,
        name_map: dict[str, str],
    ) -> Table:
        columns = self.__fetch_columns(con, dst_tbl)
        self.__cast_geometry_columns(con, src_tbl, columns)
        self.__merge(con, src_tbl, dst_tbl, columns)
        return self.__back_join(con, batch, src_tbl, name_map)
