from duckdb import DuckDBPyConnection

from . import __grid


def create_coord_to_quadrant(con: DuckDBPyConnection):
    q = """
CREATE OR REPLACE MACRO coord_to_quadrant(lon, lat) AS (
    SELECT CASE
        WHEN lon >= 0 AND lat >= 0 THEN 1
        WHEN lon < 0 AND lat >= 0 THEN 2
        WHEN lon < 0 AND lat < 0 THEN 3
        WHEN lon >=0 AND lat < 0 THEN 4
    END     
);
"""
    con.execute(q)


def create_wgs84_coord_to_grid_id(con: DuckDBPyConnection):
    create_coord_to_quadrant(con)
    __grid.create_coord_to_grid_id(con)

    q = """
CREATE OR REPLACE MACRO wgs84_coord_to_grid_id(lon, lat, cell_width, cell_height) AS (
    WITH wgs84_convert AS (
        SELECT if(lon = 180, -180, lon) + 180   AS lon_360,
               lat + 90                         AS lat_180,
    ), variable AS (
        SELECT lon_360,
               lat_180,
               coord_to_quadrant(lon_360 - 180, lat_180 - 90)  AS quadrant,
               if(lon_360 < 180, 0, 180)    AS x_min,
               if(lat_180 < 90, 0, 90)      AS y_min,
               if(lon_360 < 180, 180, 360)  AS x_max,
               if(lat_180 < 90, 90, 180)    AS y_max
        FROM wgs84_convert
    ), result AS (
        SELECT
            quadrant,
            coord_to_grid_id(
                lon_360,
                lat_180,
                x_min,
                y_min,
                x_max,
                y_max,
                cell_width,
                cell_height
            ) AS grid_id
        FROM variable
    )
    SELECT if(grid_id = -1, -1, grid_id + quadrant * 100000)
    FROM result
);
"""
    con.execute(q)


def create_grid_id_to_wgs84_coord(con: DuckDBPyConnection):
    __grid.create_grid_id_to_coord(con)

    q = """
CREATE OR REPLACE MACRO grid_id_to_wgs84_coord(grid_id, cell_width, cell_height) AS (
    WITH variable AS (
        SELECT
            floor(grid_id / 100000) AS quadrant,
            grid_id % 100000        AS v_grid_id
    ), to_coord AS (
        SELECT 
            CASE
                WHEN quadrant = 1 THEN
                    grid_id_to_coord(v_grid_id, 180, 90, 360, 180, cell_width, cell_height)
                WHEN quadrant = 2 THEN
                    grid_id_to_coord(v_grid_id, 0, 90, 180, 180, cell_width, cell_height)
                WHEN quadrant = 3 THEN
                    grid_id_to_coord(v_grid_id, 0, 0, 180, 90, cell_width, cell_height)
                WHEN quadrant = 4 THEN
                    grid_id_to_coord(v_grid_id, 180, 0, 360, 90, cell_width, cell_height)
            END AS coord
        FROM variable
    )
    SELECT {'x': coord.x - 180, 'y': coord.y - 90}
    FROM to_coord
);
"""
    con.execute(q)


def create_grid_id_to_wgs84_envelope(con: DuckDBPyConnection):
    create_grid_id_to_wgs84_coord(con)

    q = """
CREATE OR REPLACE MACRO grid_id_to_wgs84_envelope(grid_id, cell_width, cell_height) AS (
    WITH variable AS (
        SELECT grid_id_to_wgs84_coord(grid_id, cell_width, cell_height) AS coord
    )
    SELECT ST_MakeEnvelope(coord.x, coord.y, coord.x + cell_width, coord.y + cell_height)
    FROM variable
);
"""
    con.execute(q)
