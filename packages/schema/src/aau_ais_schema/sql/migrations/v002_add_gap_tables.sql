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


-- Gap Explanation Dimension
CREATE SEQUENCE IF NOT EXISTS dim.gap_explanation_dim_seq START WITH 1;

CREATE TABLE IF NOT EXISTS dim.gap_explanation_dim(
    gap_explanation_id   SMALLINT      PRIMARY KEY  DEFAULT nextval('dim.gap_explanation_dim_seq'),
    gap_explanation_name VARCHAR(250)  UNIQUE       NOT NULL
);

INSERT INTO dim.gap_explanation_dim(gap_explanation_name)
    VALUES ('unknown')
    ON CONFLICT DO NOTHING;

COMMENT ON COLUMN dim.gap_explanation_dim.gap_explanation_name IS 'Explanation of the gap, e.g. midnight or bad signal; defaults to unknown when in doubt';


-- Gap Imputed Geometry Dimension
CREATE SEQUENCE IF NOT EXISTS dim.gap_imp_geom_dim_seq START WITH 1;
CREATE TABLE IF NOT EXISTS dim.gap_imp_geom_dim(
    gap_imp_geom_id      INT                    PRIMARY KEY DEFAULT nextval('dim.gap_imp_geom_dim_seq'),
    geom                 GEOMETRY('epsg:4326')  NOT NULL    CHECK (ST_GeometryType(geom) = 'LINESTRING'),
    no_point             INT                    NOT NULL    CHECK (no_point > 1),
    length_meter         INT                    NOT NULL    CHECK (length_meter > 0),
    quad_cell_id_array   BIGINT[]               NOT NULL,
    quality_of_imp       SMALLINT               NOT NULL    CHECK (quality_of_imp BETWEEN 0 AND 100),
    dsc                  TEXT
);

INSERT INTO dim.gap_imp_geom_dim(geom, no_point, length_meter, quad_cell_id_array, quality_of_imp, dsc)
    VALUES (ST_GeomFromText('LINESTRING EMPTY'), 2, 1, ARRAY[], 0, NULL)
    ON CONFLICT DO NOTHING;

COMMENT ON COLUMN dim.gap_imp_geom_dim.quad_cell_id_array IS 'Array of cell ids covered at MVT zoom level 23';
COMMENT ON COLUMN dim.gap_imp_geom_dim.quality_of_imp IS 'Estimated quality of the imputation, 0-100';
COMMENT ON COLUMN dim.gap_imp_geom_dim.dsc IS 'Description of the individual imputation as done in Vista';


-- Gap Imputation Method Dimension
CREATE SEQUENCE IF NOT EXISTS dim.gap_imp_method_dim_seq START WITH 1;
CREATE TABLE IF NOT EXISTS dim.gap_imp_method_dim(
    gap_imp_method_id   INT             PRIMARY KEY DEFAULT nextval('dim.gap_imp_method_dim_seq'),
    method_name         VARCHAR(255)    NOT NULL    UNIQUE,
    complexity          SMALLINT        NOT NULL    CHECK (complexity BETWEEN 0 AND 100),
    dsc                 TEXT
);

INSERT INTO dim.gap_imp_method_dim(method_name, complexity)
    VALUES ('unknown', 0)
    ON CONFLICT DO NOTHING;

COMMENT ON COLUMN dim.gap_imp_method_dim.complexity IS 'Complexity of the imputation method, 0-100';
COMMENT ON COLUMN dim.gap_imp_method_dim.dsc IS 'Textual description of the imputation method';
--#endregion

--------------------------------------------------------------------------------------
--#region Facts
--------------------------------------------------------------------------------------
-- Gap Fact
-- Trajectory gap analysis: a gap between a "from" segment and a "to" segment,
-- where each segment is either a trajectory or a stop (mutually exclusive).
CREATE TABLE IF NOT EXISTS lakehouse.fact.gap_fact(
    gap_id              BIGINT                      NOT NULL,
    gap_type_id         SMALLINT                    NOT NULL,
    gap_explanation_id  SMALLINT                    NOT NULL    DEFAULT 1,
    vessel_id           INT                         NOT NULL,
    transponder_type_id SMALLINT                    NOT NULL,
    vessel_type_id      SMALLINT                    NOT NULL,
    alpha2              CHAR(2)                     NOT NULL,
    from_trajectory_id  INT,
    from_stop_id        INT,
    to_trajectory_id    INT,
    to_stop_id          INT,
    start_date_id       INT,
    start_time_id       INT,
    stop_date_id        INT,
    stop_time_id        INT,
    from_point_id       BIGINT,
    to_point_id         BIGINT,
    gap_imp_geom_id     INT                         NOT NULL    DEFAULT -1,
    gap_imp_method_id   INT,
    geom                GEOMETRY('EPSG:4326')       NOT NULL,
    duration_sec        INT                         NOT NULL,
    length_meter        INT                         NOT NULL
);

COMMENT ON COLUMN lakehouse.fact.gap_fact.from_trajectory_id IS 'From segment is a trajectory (mutually exclusive with from_stop_id); FK to lakehouse trajectory fact';
COMMENT ON COLUMN lakehouse.fact.gap_fact.from_stop_id IS 'From segment is a stop (mutually exclusive with from_trajectory_id); FK to lakehouse.fact.ais_stop_fact.ais_stop_id';
COMMENT ON COLUMN lakehouse.fact.gap_fact.to_trajectory_id IS 'To segment is a trajectory (mutually exclusive with to_stop_id); FK to lakehouse trajectory fact';
COMMENT ON COLUMN lakehouse.fact.gap_fact.to_stop_id IS 'To segment is a stop (mutually exclusive with to_trajectory_id); FK to lakehouse.fact.ais_stop_fact.ais_stop_id';
COMMENT ON COLUMN lakehouse.fact.gap_fact.from_point_id IS 'Last AIS point in the from trajectory (fact.ais_point_fact.ais_point_id)';
COMMENT ON COLUMN lakehouse.fact.gap_fact.to_point_id IS 'First AIS point in the to trajectory (fact.ais_point_fact.ais_point_id)';
COMMENT ON COLUMN lakehouse.fact.gap_fact.geom IS 'Straight line from the last point of the from segment to the first point of the to segment (for visualization)';
COMMENT ON COLUMN lakehouse.fact.gap_fact.duration_sec IS 'Duration of the gap in seconds';
COMMENT ON COLUMN lakehouse.fact.gap_fact.length_meter IS 'Length of the gap in meters';
--#endregion
--------------------------------------------------------------------------------------