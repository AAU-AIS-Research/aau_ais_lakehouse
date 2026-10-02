--------------------------------------------------------------------------------------
--#region Dimensions
--------------------------------------------------------------------------------------
-- Gap Type Dimension
CREATE SEQUENCE IF NOT EXISTS dim.gap_type_dim_seq START WITH 1;
CREATE TABLE IF NOT EXISTS dim.gap_type_dim(
    gap_type_id   SMALLINT      PRIMARY KEY  DEFAULT nextval('dim.gap_type_dim_seq'),
    short_name    VARCHAR(250)  UNIQUE       NOT NULL,
    long_name     VARCHAR(250)  UNIQUE       NOT NULL,
    dsc           VARCHAR(250)
);

INSERT INTO dim.gap_type_dim(short_name, long_name, dsc)
    VALUES ('uk', 'Unknown', 'Unknown gap type'),
           ('tt', 'Trajectory to trajectory', 'Trajectory connected to trajectory via gap'),
           ('ss', 'Stop to stop','Stop connected to stop via gap'),
           ('st', 'Stop to trajectory', 'Stop connected to trajectory via gap'),
           ('ts', 'trajectory to stop', 'Trajectory connected to stop via gap')
    ON CONFLICT DO NOTHING;

-- Gap Imputed Geometry Dimension
CREATE TABLE IF NOT EXISTS dim.gap_imp_mv_dim(
    gap_imp_id          INT,
    method_name         VARCHAR                 NOT NULL    CHECK (length(method_name) > 0),
    dsc                 TEXT,
    no_point            INT                     NOT NULL    CHECK (no_point > 1),
    meters              INT                     NOT NULL    CHECK (meters >= 0),
    tortuosity          FLOAT                   NOT NULL    CHECK (tortuosity >= 1),
    is_simple_geom      BOOLEAN                 NOT NULL,
    is_valid_geom       BOOLEAN                 NOT NULL,
    quad_cell_id_array  BIGINT[]                NOT NULL,
    quality_of_imp      SMALLINT                NOT NULL    CHECK (quality_of_imp BETWEEN 0 AND 100),
    method_complexity   SMALLINT                NOT NULL    CHECK (method_complexity BETWEEN 0 AND 100),
    start_point         GEOMETRY('EPSG:4326')   NOT NULL    CHECK (ST_GeometryType(start_point) = 'POINT'),
    end_point           GEOMETRY('EPSG:4326')   NOT NULL    CHECK (ST_GeometryType(end_point) = 'POINT'),
    geom                GEOMETRY('EPSG:4326')   NOT NULL    CHECK (ST_GeometryType(geom) = 'LINESTRING'),
    PRIMARY KEY (gap_imp_id, method_name)
);

COMMENT ON COLUMN dim.gap_imp_mv_dim.quad_cell_id_array IS 'Array of cell ids covered at MVT zoom level 23';
COMMENT ON COLUMN dim.gap_imp_mv_dim.quality_of_imp IS 'Estimated quality of the imputation, 0-100';
COMMENT ON COLUMN dim.gap_imp_mv_dim.method_complexity IS 'Complexity of the imputation method, 0-100';
COMMENT ON COLUMN dim.gap_imp_mv_dim.dsc IS 'Textual description of the imputation method';


INSERT INTO dim.gap_imp_mv_dim(
        gap_imp_id,
        method_name,
        dsc,
        no_point,
        meters,
        tortuosity,
        is_simple_geom,
        is_valid_geom,
        quad_cell_id_array,
        quality_of_imp,
        method_complexity,
        start_point,
        end_point,
        geom
    )
    VALUES (
        1,
        'empty',
        'This is the default empty imputation method used when imputation has not yet been performed',
        2,
        0,
        1,
        false,
        false,
        ARRAY[],
        0,
        0,
        ST_GeomFromText('POINT EMPTY'),
        ST_GeomFromText('POINT EMPTY'),
        ST_GeomFromText('LINESTRING EMPTY')
    )
    ON CONFLICT DO NOTHING;
--#endregion

--------------------------------------------------------------------------------------
--#region Facts
--------------------------------------------------------------------------------------
-- Gap Fact
-- Trajectory gap analysis: a gap between a "from" segment and a "to" segment,
-- where each segment is either a trajectory or a stop (mutually exclusive).
CREATE TABLE IF NOT EXISTS lakehouse.fact.ais_gap_fact(
    ais_gap_id              BIGINT                      NOT NULL,
    src_id                  UINTEGER                    NOT NULL,
    load_id                 UINTEGER                    NOT NULL,
    gap_type_id             SMALLINT                    NOT NULL,
    state_change_id         SMALLINT                    NOT NULL    DEFAULT 1,
    vessel_id               INT                         NOT NULL,
    transponder_type_id     SMALLINT                    NOT NULL,
    vessel_type_id          SMALLINT                    NOT NULL,
    gap_imp_id              INT                         NOT NULL    DEFAULT 1,
    alpha2                  CHAR(2)                     NOT NULL,
    start_traj_id           INT,
    start_stop_id           INT,
    end_traj_id             INT,
    end_stop_id             INT,
    start_date_id           INT,
    start_time_id           INT,
    end_date_id             INT,
    end_time_id             INT,
    start_point_id          BIGINT,
    end_point_id            BIGINT,
    geom                    GEOMETRY('EPSG:4326')       NOT NULL,
    seconds                 INT                         NOT NULL,
    meters                  INT                         NOT NULL,
    required_avg_speed_kt   INT                         NOT NULL
);

COMMENT ON COLUMN lakehouse.fact.ais_gap_fact.start_traj_id IS 'From segment is a trajectory (mutually exclusive with start_stop_id); FK to lakehouse trajectory fact';
COMMENT ON COLUMN lakehouse.fact.ais_gap_fact.start_stop_id IS 'From segment is a stop (mutually exclusive with start_trajectory_id); FK to lakehouse.fact.ais_stop_fact.ais_stop_id';
COMMENT ON COLUMN lakehouse.fact.ais_gap_fact.end_traj_id IS 'To segment is a trajectory (mutually exclusive with end_stop_id); FK to lakehouse trajectory fact';
COMMENT ON COLUMN lakehouse.fact.ais_gap_fact.end_stop_id IS 'To segment is a stop (mutually exclusive with end_trajectory_id); FK to lakehouse.fact.ais_stop_fact.ais_stop_id';
COMMENT ON COLUMN lakehouse.fact.ais_gap_fact.start_point_id IS 'Last AIS point in the from trajectory (fact.ais_point_fact.ais_point_id)';
COMMENT ON COLUMN lakehouse.fact.ais_gap_fact.end_point_id IS 'First AIS point in the to trajectory (fact.ais_point_fact.ais_point_id)';
COMMENT ON COLUMN lakehouse.fact.ais_gap_fact.geom IS 'Straight line from the last point of the from segment to the first point of the to segment (for visualization)';
COMMENT ON COLUMN lakehouse.fact.ais_gap_fact.seconds IS 'Duration of the gap in seconds';
COMMENT ON COLUMN lakehouse.fact.ais_gap_fact.meters IS 'Length of the gap in meters';
COMMENT ON COLUMN lakehouse.fact.ais_gap_fact.required_avg_speed_kt IS 'The required average speed, in knots, for traveling between start and end point';
--#endregion
--------------------------------------------------------------------------------------