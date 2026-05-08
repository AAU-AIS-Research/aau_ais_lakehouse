import asyncio
from dataclasses import dataclass
from pathlib import Path

import duckdb
from adbc_driver_manager.dbapi import Connection
from duckdb import DuckDBPyConnection, DuckDBPyRelation


@dataclass(slots=True, frozen=True)
class ColumnInfo:
    name: str
    type: str
    null: str
    key: str
    default: str
    extra: str


def pg_query(con: DuckDBPyConnection, db: str, sql: str):
    return con.query(f"SELECT * FROM postgres_query('{db}', $${sql}$$);")


async def pg_query_async(
    con: DuckDBPyConnection, db: str, sql: str, result_tbl: str
) -> DuckDBPyRelation:
    """
    Asynchronously queries a PostgreSQL database through a DuckDB connection.
    Arguments:
    con (DuckDBPyConnection): The DuckDB connection to use.
    db (str): The PostgreSQL database name.
    sql (str): The SQL query string to execute.
    result_tbl (str): The name of the target DuckDB table to store the query result.
    Returns:
    None: The result of the query is saved in the specified DuckDB table.
    This function utilizes asyncio to perform blocking operations in a
    dedicated thread, ensuring the main thread remains responsive.
    The result table is necessary as the DuckDBPyRelation is destroyed when exiting the thread.
    """

    def __aquery(con: DuckDBPyConnection, db: str, sql: str, result_tbl: str) -> None:
        con = con.cursor()
        pg_query(con, db, sql).to_table(result_tbl)

    await asyncio.to_thread(__aquery, con, db, sql, result_tbl)
    return con.table(result_tbl)


def pg_execute(con: DuckDBPyConnection, db: str, sql: str) -> None:
    con.execute(f"CALL postgres_execute('{db}', $${sql}$$);")


async def pg_execute_async(con: DuckDBPyConnection, db: str, sql: str) -> None:
    def __aexecute(con: DuckDBPyConnection, db: str, sql: str) -> None:
        con = con.cursor()
        pg_execute(con, db, sql)

    await asyncio.to_thread(__aexecute, con, db, sql)


async def query_async(
    con: DuckDBPyConnection, sql: str, result_tbl: str, params: object = None
) -> DuckDBPyRelation:
    def __aquery(
        con: DuckDBPyConnection, sql: str, result_tbl: str, params: object
    ) -> None:
        con = con.cursor()
        con.query(sql, params=params).to_table(result_tbl)

    await asyncio.to_thread(__aquery, con, sql, result_tbl, params)
    return con.table(result_tbl)


async def execute_async(
    con: DuckDBPyConnection, sql: str, params: object = None
) -> None:
    def __aexecute(con: DuckDBPyConnection, sql: str) -> None:
        con = con.cursor()
        con.execute(sql, parameters=params)

    await asyncio.to_thread(__aexecute, con, sql)


def to_tmp_table(con: DuckDBPyConnection, rel: DuckDBPyRelation, name: str):
    con.execute(f"CREATE TABLE {name} AS SELECT * FROM rel;")


def pg_disable_all_triggers(con: DuckDBPyConnection, db: str, tbl: str):
    pg_execute(
        con,
        db,
        f"ALTER TABLE {tbl} DISABLE TRIGGER ALL;",
    )


def pg_enable_all_triggers(con: DuckDBPyConnection, db: str, tbl: str):
    pg_execute(
        con,
        db,
        f"ALTER TABLE {tbl} ENABLE TRIGGER ALL;",
    )


def get_spatial_con(
    database: str | Path = ":memory:",
    read_only: bool = False,
    config: dict[str, str | bool | int | float | list[str]] = {},
) -> DuckDBPyConnection:
    con = duckdb.connect(database=database, read_only=read_only, config=config)
    con.install_extension("spatial")
    con.load_extension("spatial")
    # con.execute("CALL register_geoarrow_extensions();")

    return con


def fetch_columns(con: Connection, dst_tbl: str):
    q = f"""--sql
select
    column_name,
    column_type,
    "null",
    "key",
    "default",
    extra
from (describe {dst_tbl});
"""
    with con.cursor() as curs:
        res = curs.execute(q).fetchall()
        if len(res) == 0:
            raise ValueError(f"{dst_tbl} does not seem to exist")
        return [ColumnInfo(*columns) for columns in res]
