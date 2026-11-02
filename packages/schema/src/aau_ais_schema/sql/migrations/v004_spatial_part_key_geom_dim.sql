--------------------------------------------------------------------------------------
--#region Dimensions
--------------------------------------------------------------------------------------
-- Spatial partition key geometry dimension
-- Contains one row per spatial partition i.e. per quadkey at the partition depth,
-- keyed by the int-encoded quadkey prefix, matching lakehouse.fact.ais_point_fact.spatial_part_key
CREATE TABLE IF NOT EXISTS dim.spatial_part_key_geom_dim(
    spatial_part_key  UINTEGER    PRIMARY KEY,
    quadkey           VARCHAR     NOT NULL    UNIQUE,
    level             UTINYINT    NOT NULL,
    x                 UINTEGER    NOT NULL,
    y                 UINTEGER    NOT NULL,
    geom              GEOMETRY    NOT NULL    CHECK (ST_GeometryType(geom) = 'POLYGON'),
    area              DOUBLE      NOT NULL    CHECK (area > 0),
    centroid          GEOMETRY    NOT NULL    CHECK (ST_GeometryType(centroid) = 'POINT')
);

COMMENT ON COLUMN dim.spatial_part_key_geom_dim.spatial_part_key is 'Int-encoded quadkey prefix at the partition depth (quadkey_uint64_encode), matches lakehouse.fact.ais_point_fact.spatial_part_key. Defaults to depth 8 (aau_ais_core.quadkey_utils.DEFAULT_PARTITION_DEPTH), chosen by the loader, max 16 (uinteger width)';
COMMENT ON COLUMN dim.spatial_part_key_geom_dim.quadkey is 'String quadkey at the partition depth, e.g. ''12002333''';
COMMENT ON COLUMN dim.spatial_part_key_geom_dim.level is 'Quadkey zoom level, equals the partition depth';
COMMENT ON COLUMN dim.spatial_part_key_geom_dim.x is 'Tile column at the partition depth';
COMMENT ON COLUMN dim.spatial_part_key_geom_dim.y is 'Tile row at the partition depth';
COMMENT ON COLUMN dim.spatial_part_key_geom_dim.geom is 'Partition cell envelope in EPSG:4326';
COMMENT ON COLUMN dim.spatial_part_key_geom_dim.area is 'Cell area in square meters (geodesic, WGS84 spheroid)';
COMMENT ON COLUMN dim.spatial_part_key_geom_dim.centroid is 'Cell centroid in EPSG:4326';
--#endregion
--------------------------------------------------------------------------------------
