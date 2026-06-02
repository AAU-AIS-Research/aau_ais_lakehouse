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
        self,
        sequence: str,
        surrogate_key: str,
        keys: Sequence[str],
        attributes: Sequence[str] = [],
    ):
        self.__sequence = sequence
        self.__surrogate_key = surrogate_key
        self.__keys = keys
        self.__attributes = attributes

    @property
    def sequence(self) -> str:
        return self.__sequence

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
-- Join existing surrogate keys into staging table
create or replace temporary table {{src_tbl}} as
    select dst.{{surrogate_key}}, *
    from {{src_tbl}} as src
        left join {{dst_tbl}} as dst on (
            {%- for key in keys %}
            {% if not loop.first %}and {% endif %}dst.{{key}} is not distinct from src.{{key}}
            {%- endfor %}
        );

-- Insert new rows into destination table
insert into {{dst_tbl}} ({{surrogate_key}}, {{(keys + attributes) | join(', ')}})
    select
        nextval('{{sequence}}') as {{surrogate_key}},
        {{(keys + attributes) | join(', ')}}
    from {{src_tbl}}
    where {{surrogate_key}} is null;

create or replace temporary table {{src_tbl}} as
    select dst.{{surrogate_key}}, *
    from {{src_tbl}} as src
        inner join {{dst_tbl}} as dst on (
            {%- for key in keys %}
            {% if not loop.first %}and {% endif %}dst.{{key}} is not distinct from src.{{key}}
            {%- endfor %}
        );

-- Update staging table with newly inserted surrogate keys
--update {{src_tbl}} as src
--    set {{surrogate_key}} = dst.{{surrogate_key}}
--    from {{dst_tbl}} as dst
--    where src.{{surrogate_key}} is null;
"""
        q = Template(template_str).render(
            src_tbl=src_tbl,
            dst_tbl=dst_tbl,
            surrogate_key=self.surrogate_key,
            keys=self.keys,
            attributes=self.attributes,
            sequence=self.sequence,
        )

        with con.cursor() as curs:
            curs.execute(q)

    def __fetch(self, con: Connection, src_tbl: str, dst_tbl: str) -> Table:
        template_str = """--sql
select distinct
    {{surrogate_key}}
    {%- for key in keys %}
    ,{{key}}
    {%- endfor %}
from {{src_tbl}};
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
