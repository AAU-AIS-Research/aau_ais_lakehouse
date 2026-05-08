from collections.abc import Sequence
from typing import Callable

import duckdb
from adbc_driver_manager.dbapi import Connection
from jinja2 import Template
from pyarrow import Table

Processor = Callable[[Table, dict[str, str]], Table]
MergeStrategy = Callable[[Connection, Table, str, str, dict[str, str]], Table]


class SurrogateKeyMergeStrategy:
    """
    A merge strategy that matches existing records using key columns and inserts new ones.

    This strategy uses a SQL MERGE statement to match records based on the provided key columns.
    If a record exists, it is updated (or ignored, depending on the database). If it doesn't
    exist, it is inserted. After the merge, the surrogate key is joined back to the local
    dataset for further processing.
    """

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

    def __merge(self, con: Connection, src_tbl: str, dst_tbl: str) -> None:
        template_str = """--sql
merge into {{dst_tbl}} as dst
    using {{src_tbl}} as src
    on (
        {%- for key in keys %}
        {% if not loop.first %}and {% endif %}dst.{{key}} is not distinct from src.{{key}}
        {%- endfor %}
    )
    when not matched then 
        insert ({{surrogate_key}}, {{(keys + attributes) | join(', ')}})
        values (DEFAULT, {{(keys + attributes) | map('replace', '', 'src.', 1) | join(', ')}});
"""
        q = Template(template_str).render(
            src_tbl=src_tbl,
            dst_tbl=dst_tbl,
            surrogate_key=self.surrogate_key,
            keys=self.keys,
            attributes=self.attributes,
        )

        with con.cursor() as curs:
            curs.execute(q)

    def __fetch(self, con: Connection, src_tbl: str, dst_tbl: str) -> Table:
        template_str = """--sql
select
    r.{{surrogate_key}}
    {%- for key in keys %}
    ,l.{{key}}
    {%- endfor %}
from {{src_tbl}} as l
    inner join {{dst_tbl}} as r on
        {%- for key in keys %}
        {% if not loop.first %}and {% endif %}l.{{key}} is not distinct from r.{{key}}
        {%- endfor %};
"""
        q = Template(template_str).render(
            src_tbl=src_tbl,
            dst_tbl=dst_tbl,
            surrogate_key=self.surrogate_key,
            keys=self.keys,
        )

        with con.cursor() as curs:
            return curs.execute(q).fetch_arrow_table()

    def __call__(
        self,
        con: Connection,
        batch: Table,
        src_tbl: str,
        dst_tbl: str,
        name_map: dict[str, str],
    ) -> Table:
        self.__merge(con, src_tbl, dst_tbl)
        dim = self.__fetch(con, src_tbl, dst_tbl)

        with duckdb.connect().begin() as local_con:
            template_str = """--sql
select
    batch.*,
    {{surrogate_key}} {% if surrogate_key in name_map %}as {{name_map[surrogate_key]}}{% endif %}
from batch
    inner join dim on
        {%- for key in keys %}
        {% if not loop.first %}and {% endif %}batch.{{name_map[key]}} is not distinct from dim.{{key}}
        {%- endfor %};
"""
            q = Template(template_str).render(
                surrogate_key=self.surrogate_key,
                keys=self.keys,
                name_map=name_map,
            )
            return local_con.query(q).fetch_arrow_table()
