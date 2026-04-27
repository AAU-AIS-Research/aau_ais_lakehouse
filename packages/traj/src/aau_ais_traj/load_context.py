import time

from adbc_driver_flightsql.dbapi import Connection
from loguru import logger

from aau_ais_traj.exceptions import LoadError


class _LoadContext:
    def __init__(self, con: Connection, id: int, src_id: str, dst_tbl: str) -> None:
        self.__con = con
        self.__id = id
        self.__src_id = src_id
        self.__dst_tbl = dst_tbl

    @property
    def id(self) -> int:
        return self.__id

    @property
    def src_id(self) -> str:
        return self.__src_id

    def ingest_started(self):
        q = """--sql
update dim.load_dim
set ingest_start_ts = to_timestamp(?)
where load_id = ?;
"""
        with self.__con.cursor() as curs:
            epoch = time.time()
            curs.execute(q, parameters=[epoch, self.id]).fetchall()

    def ingest_stopped(self):
        q = f"""--sql
update dim.load_dim
set ingest_end_ts = to_timestamp(?),
    ingest_duration = to_timestamp(?)::timestamp - ingest_start_ts,
    ingest_per_sec = (
        select count(*) / epoch(to_timestamp(?)::timestamp - ingest_start_ts)
        from {self.__dst_tbl}
        where load_id = ?
    )
where load_id = ?;
"""
        with self.__con.cursor() as curs:
            epoch = time.time()
            curs.execute(
                q, parameters=[epoch, epoch, epoch, self.id, self.id]
            ).fetchall()


class LoadContext:
    def __init__(self, src_id: str, dst_tbl: str, con: Connection) -> None:
        self.__src_id = src_id
        self.__dst_tbl = dst_tbl
        self.__load_id = -1
        self.__con = con
        self.__start_time = None

    @property
    def load_id(self) -> int:
        return self.__load_id

    @staticmethod
    def is_loaded(src_id, dst_tbl, con: Connection) -> bool:
        q = """--sql
select exists (
    select 1
    from dim.load_dim
    where src_id = ?
        and dst_tbl = ?
        and failed = false
        and deleted = false
);
"""
        with con.cursor() as curs:
            curs.execute(q, parameters=[src_id, dst_tbl])
            return curs.fetchall()[0][0]

    def __is_loaded(self) -> bool:
        return self.is_loaded(self.__src_id, self.__dst_tbl, self.__con)

    def __roleback(self):
        self.__con.rollback()

    def __register(self) -> int:
        q = """--sql
insert into dim.load_dim (
    load_id,
	src_id,
    dst_tbl,
	start_ts,
    deleted,
    failed,
    no_rows,
    end_ts,
    total_duration,
    ingest_start_ts,
    ingest_end_ts,
    ingest_duration,
    ingest_per_sec
)
values (
    (select coalesce(max(load_id), 0) + 1 from dim.load_dim),
    ?,
    ?,
    to_timestamp(?),
    false,
    true,
    null,
    null,
    null,
    null,
    null,
    null,
    null
)
returning load_id;
"""
        with self.__con.cursor() as curs:
            result = curs.execute(
                q, parameters=[self.__src_id, self.__dst_tbl, time.time()]
            ).fetchall()
            return result[0][0]

    def start(self) -> _LoadContext:
        logger.info("Starting load...")
        self.__start_time = time.perf_counter()

        try:
            if self.__is_loaded():
                raise LoadError(f'Source: "{self.__src_id}" has already been loaded')
            self.__load_id = self.__register()
            self.__con.commit()
            logger.info("Load {} started!", self.load_id)
        except Exception as e:
            self.__roleback()
            raise e

        return _LoadContext(self.__con, self.load_id, self.__src_id, self.__dst_tbl)

    def stop(self):
        logger.info("Stopping load...")
        q = f"""--sql
update dim.load_dim
set end_ts = to_timestamp(?),
    total_duration = to_timestamp(?)::timestamp - start_ts,
    no_rows = (
        select count(*)
        from {self.__dst_tbl}
        where load_id = ?
    ),
    failed = false
where load_id = ?;"""
        with self.__con.cursor() as curs:
            epoch = time.time()
            curs.execute(
                q, parameters=[epoch, epoch, self.load_id, self.load_id]
            ).fetchall()
            self.__con.commit()
        logger.info(
            "Load {} stopped in {:,.2f}s!",
            self.load_id,
            time.perf_counter() - self.__start_time if self.__start_time else -1,
        )

    def fail(self):
        q = f"""--sql
update dim.load_dim
set end_ts = to_timestamp(?),
    total_duration = to_timestamp(?)::timestamp - start_ts,
    no_rows = (
        select count(*)
        from {self.__dst_tbl}
        where load_id = ?
    ),
    failed = true
where load_id = ?;"""
        with self.__con.cursor() as curs:
            self.__roleback()

            epoch = time.time()
            curs.execute(
                q, parameters=[epoch, epoch, self.load_id, self.load_id]
            ).fetchall()
            self.__con.commit()

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None and exc_val is None and exc_tb is None:
            try:
                self.stop()
            except Exception as e:
                self.fail()
                raise e
        else:
            self.fail()

        self.__con.commit()
