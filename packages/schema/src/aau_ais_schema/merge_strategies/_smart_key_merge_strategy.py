from adbc_driver_manager.dbapi import Connection
from jinja2 import Template
from pyarrow import Table


class SmartKeyMergeStrategy:
    """
    A merge strategy that inserts rows where their smart key is not yet pressent

    This strategy assumes that the smart key is already present in the local data source,
    so it simply inserts all non-existing data into the dimension. The batch is returned
    as-is, without any additional columns.

    This is suitable for dimensions where the smart key is already present in the source
    data and no surrogate key generation is needed.
    """

    def __call__(
        self,
        con: Connection,
        batch: Table,
        src_tbl: str,
        dst_tbl: str,
        name_map: dict[str, str],
    ) -> Table:
        # We only need to insert data as smart keys should already be on the data source
        template_str = """--sql
insert or ignore into {{dst_tbl}} by name (
    select * from {{src_tbl}}
);
"""
        q = Template(template_str).render(src_tbl=src_tbl, dst_tbl=dst_tbl)
        con.execute(q)
        return batch
