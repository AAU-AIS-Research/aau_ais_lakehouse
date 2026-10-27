import datetime
import logging
import time
from datetime import datetime as Datetime

from adbc_driver_manager.dbapi import Connection

from aau_ais_schema.exceptions import LoadError

logger = logging.getLogger(__name__)


class IngestionContext:
    def __init__(self, con: Connection, id: int, src_id: str, dst_tbl: str) -> None:
        self.__con = con
        self.__id = id
        self.__src_id = src_id
        self.__dst_tbl = dst_tbl
        self.__start_ingest_datetime: Datetime | None = None
        self.__stop_ingest_datetime: Datetime | None = None

    @property
    def connection(self) -> Connection:
        return self.__con

    @property
    def id(self) -> int:
        return self.__id

    @property
    def src_id(self) -> str:
        return self.__src_id

    @property
    def start_ingest_datetime(self) -> Datetime | None:
        return self.__start_ingest_datetime

    @property
    def stop_ingest_datetime(self) -> Datetime | None:
        return self.__stop_ingest_datetime

    @property
    def completed(self) -> bool:
        return (
            self.start_ingest_datetime is not None
            and self.stop_ingest_datetime is not None
        )

    def ingest_started(self):
        self.__start_ingest_datetime = Datetime.now(datetime.UTC)

    def ingest_stopped(self):
        self.__stop_ingest_datetime = Datetime.now(datetime.UTC)


class LoadContext:
    def __init__(self, src_id: str, dst_tbl: str, con: Connection) -> None:
        self.__src_id = src_id
        self.__dst_tbl = dst_tbl
        self.__load_id = -1
        self.__con = con
        self.__start_time = None
        self.__ctx: IngestionContext | None = None

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

    def start(self) -> IngestionContext:
        logger.info("Starting load...")
        self.__start_time = time.perf_counter()

        try:
            if self.__is_loaded():
                raise LoadError(f'Source: "{self.__src_id}" has already been loaded')
            self.__load_id = self.__register()
            self.__con.commit()
            logger.info("Load %s started!", self.load_id)
        except Exception as e:
            self.__roleback()
            raise e
        self.__ctx = IngestionContext(
            self.__con, self.load_id, self.__src_id, self.__dst_tbl
        )
        return self.__ctx

    def __update_tbl(self, failed: bool):
        if self.__ctx is None:
            raise ValueError("Context never started")

        q = f"""--sql
with load_stats as (
    select
        $2                                                                          as load_id,
        count(*)                                                                    as no_rows,
        $3                                                                          as ingest_start_ts,
        $4                                                                          as ingest_end_ts,
        ingest_end_ts - ingest_start_ts                                             as ingest_duration
        if(epoch(ingest_duration) = 0, no_rows, no_rows / epoch(ingest_duration))   as ingest_per_sec
    from {self.__dst_tbl}
    where load_id = $2
)
update dim.load_dim as d
set end_ts = $1,
    total_duration = $1 - start_ts,
    no_rows = s.no_rows,
    ingest_start_ts = s.ingest_start_ts,
    ingest_end_ts = s.ingest_end_ts,
    ingest_duration = s.ingest_duration,
    ingest_per_sec = s.ingest_per_sec,
    failed = $5
from load_stats as s
where d.load_id = s.load_id;"""
        with self.__con.cursor() as curs:
            now = Datetime.now(datetime.UTC)
            curs.execute(
                q,
                parameters=[
                    now,
                    self.load_id,
                    self.__ctx.start_ingest_datetime,
                    self.__ctx.stop_ingest_datetime,
                    failed,
                ],
            ).fetchall()
            self.__con.commit()

    def stop(self):
        logger.info("Stopping load...")
        if self.__ctx and not self.__ctx.completed:
            raise RuntimeError(
                "Ingestion is either not started, stopped or both! Remember to call ingest_started() and ingest_stopped"
            )
        self.__update_tbl(failed=False)
        logger.info(
            "Load %s stopped in %.2fs!",
            self.load_id,
            time.perf_counter() - self.__start_time if self.__start_time else -1,
        )

    def fail(self):
        if self.__ctx is None:
            raise ValueError("Context never started")
        logger.error("Load failed, performing roleback")
        self.__roleback()
        logger.info("Roleback completed")
        logger.info("Wrapping up load dimension")
        self.__update_tbl(failed=True)
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
