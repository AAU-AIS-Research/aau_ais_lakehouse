from aau_ais_core import duckdb_macros, duckdb_utils
from aau_ais_core.quadkey_utils import DEFAULT_PARTITION_DEPTH
from adbc_driver_manager.dbapi import Connection
from pyarrow import Table

from aau_ais_schema.dim.__dimension import Dimension, Processor
from aau_ais_schema.merge_strategies import SmartKeyMergeStrategy

# Max quadkey depth that fits in the uinteger (32 bit) spatial_part_key column
MAX_PARTITION_DEPTH = 16


class SpatialPartKeyGeomFieldExpander:
    """
    Derives the string quadkey, tile coordinates and geometry columns from the
    int-encoded spatial_part_key. The key only holds quadkeys at the partition
    depth, which cannot be recovered from the int alone (leading zero digits
    are lost in the encoding), so it defaults to DEFAULT_PARTITION_DEPTH and can
    be overridden by the caller.
    """

    def __init__(self, partition_depth: int = DEFAULT_PARTITION_DEPTH):
        if not 1 <= partition_depth <= MAX_PARTITION_DEPTH:
            raise ValueError(
                f"partition_depth must be in [1, {MAX_PARTITION_DEPTH}], got {partition_depth}"
            )
        self.partition_depth = partition_depth

    def __call__(self, batch: Table, name_map: dict[str, str]) -> Table:
        spk = name_map["spatial_part_key"]
        depth = self.partition_depth
        q = f"""--sql
with tiles as (
    select
        b.{spk}                                   as spatial_part_key,
        int_to_quadkey(b.{spk}, {depth})          as quadkey,
        t.z::utinyint                             as level,
        t.x::uinteger                             as x,
        t.y::uinteger                             as y
    from batch b, lateral (
        select z, x, y from quadkey_to_zxy(int_to_quadkey(b.{spk}, {depth}))
    ) as t
),
bounds as (
    select
        spatial_part_key,
        quadkey,
        level,
        x,
        y,
        x / pow(2, level) * 360.0 - 180.0                                as lon_min,
        (x + 1) / pow(2, level) * 360.0 - 180.0                          as lon_max,
        degrees(atan(sinh(pi() * (1 - 2.0 * (y + 1) / pow(2, level)))))  as lat_min,
        degrees(atan(sinh(pi() * (1 - 2.0 * y / pow(2, level)))))        as lat_max
    from tiles
),
cells as (
    select
        spatial_part_key,
        quadkey,
        level,
        x,
        y,
        ST_MakeEnvelope(lon_min, lat_min, lon_max, lat_max) as geom
    from bounds
)
select
    spatial_part_key,
    quadkey,
    level,
    x,
    y,
    geom,
    ST_Area(ST_Transform(geom, 'EPSG:4326', 'EPSG:3857')) as area,
    ST_Centroid(geom)                                     as centroid
from cells;
"""
        with duckdb_utils.get_spatial_con().begin() as con:
            duckdb_macros.create_int_to_quadkey(con)
            duckdb_macros.create_quadkey_to_zxy(con)
            return con.query(q).to_arrow_table()


class SpatialPartKeyGeomDim(Dimension):
    """
    Partition cell dimension, loaded lazily as ais_point_fact rows reference new
    spatial_part_key values (SmartKeyMergeStrategy inserts only missing keys).

    The partition_depth defaults to DEFAULT_PARTITION_DEPTH and can be
    overridden, but must match the depth used when the spatial_part_key values
    were produced (max 16, the uinteger width).

    Use it to resolve which partitions to query for an arbitrary geometry, e.g.:
        select spatial_part_key from dim.spatial_part_key_geom_dim
        where ST_Intersects(geom, ST_GeomFromText(?, 4326));
    """

    def __init__(
        self,
        con: Connection,
        partition_depth: int = DEFAULT_PARTITION_DEPTH,
        catalog_name: str = "ais",
        schema_name: str = "dim",
        table_name: str = "spatial_part_key_geom_dim",
        pre_processors: list[Processor] = [],
    ):
        columns = [
            "spatial_part_key",
            "quadkey",
            "level",
            "x",
            "y",
            "geom",
            "area",
            "centroid",
        ]
        super().__init__(
            con,
            catalog_name,
            schema_name,
            table_name,
            columns,
            SmartKeyMergeStrategy(),
            [SpatialPartKeyGeomFieldExpander(partition_depth), *pre_processors],
        )
