import time

from adbc_driver_manager.dbapi import Connection
from loguru import logger
from pyarrow import RecordBatchReader, Table


def __ingest(data: RecordBatchReader, dst_con: Connection) -> Table:
    tmp_tbl = "src_stop_geom_dim"
    with dst_con.cursor() as curs:
        curs.adbc_ingest(tmp_tbl, data, mode="replace", temporary=True)

        q = f"""--sql
alter table {tmp_tbl}
    add column if not exists geom_id uinteger;
alter table {tmp_tbl}
    add column if not exists is_new boolean default true;

update {tmp_tbl} as src
    set geom_id = nextval('dim.stop_geom_dim_geom_id_seq');

insert into dim.stop_geom_dim by name
    select
        geom_id,
        ST_GeomFromWkb(geom)                    as geom,
        ST_GeomFromWkb(start_point)             as start_point,
        ST_GeomFromWkb(end_point)               as end_point,
        is_simple_geom,
        is_valid_geom,
        ST_GeomFromWkb(centroid)                as centroid,
        ST_GeomFromWkb(simplified_geom)         as simplified_geom,
        ST_GeomFromWkb(simplified_geom_topo)    as simplified_geom_topo
    from {tmp_tbl};
    """
        curs.execute(q)
        return curs.execute(
            f"select ais_stop_id, geom_id, is_new from {tmp_tbl};"
        ).fetch_arrow_table()


def load(dst_con: Connection, data: RecordBatchReader) -> Table:
    logger.info("Loading stop_geom_dim...")

    s = time.perf_counter()
    result = __ingest(data, dst_con)
    logger.info(
        "Inserted {:,} row(s) into stop_geom_dim in {:,.2f}s",
        len(result),
        time.perf_counter() - s,
    )

    return result
