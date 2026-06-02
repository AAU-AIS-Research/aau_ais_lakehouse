import logging
import math
from typing import Literal

from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql.dbapi import Connection
from pyarrow import Table

logger = logging.getLogger(__name__)


def generate_sequence_name(
    schema_name: str,
    table_name: str,
    surrogate_key_name: str,
    prefix: str = "",
    postfix: str = "_seq",
) -> str:
    return f"{schema_name}.{prefix}{table_name}_{surrogate_key_name}{postfix}"


def flight_sql_ingest(
    con: Connection,
    table_name: str,
    data: Table,
    mode: Literal["append", "create", "replace", "create_append"] = "create",
    catalog_name: str | None = None,
    db_schema_name: str | None = None,
    temporary: bool = False,
):
    try:
        max_msg_bytes = con.adbc_database.get_option_int(
            DatabaseOptions.WITH_MAX_MSG_SIZE.value
        )
    except Exception:
        max_msg_bytes = 16 * 1024 * 1024
        logger.debug(
            "{} not set, defaulting to Flight SQL default: {}",
            "WITH_MAX_MSG_SIZE",
            max_msg_bytes,
        )

    size = data.nbytes
    splits = math.ceil(size / max_msg_bytes)
    max_chunk_size = math.floor(len(data) / splits)
    reader = data.to_reader(max_chunk_size)

    with con.cursor() as cursor:
        cursor.adbc_ingest(
            table_name=table_name,
            data=reader,
            mode=mode,
            catalog_name=catalog_name,
            db_schema_name=db_schema_name,
            temporary=temporary,
        )
