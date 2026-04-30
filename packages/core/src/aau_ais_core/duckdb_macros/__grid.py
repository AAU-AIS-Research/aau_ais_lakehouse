from duckdb import DuckDBPyConnection


def create_coord_to_grid_id(con: DuckDBPyConnection):
    q = """--sql
CREATE OR REPLACE MACRO coord_to_grid_id(x, y, x_min, y_min, x_max, y_max, cell_width, cell_height) AS (
    WITH variables AS (
        SELECT floor((x_max - x_min) / cell_width)  AS col_cnt,
               floor((y_max - y_min) / cell_height) AS row_cnt,
               floor((x - x_min) / cell_width)          AS i,
               floor((y - y_min) / cell_height)         AS j
    )
    SELECT
        CASE
            WHEN 0 > i OR i >= col_cnt OR 0 > j OR j >= row_cnt OR isinf(x) OR isinf(y) THEN -1
            ELSE (col_cnt * j + i)::BIGINT
        END AS grid_id
    FROM variables
);
"""
    con.execute(q)


def create_point_to_grid_id(con: DuckDBPyConnection):
    create_coord_to_grid_id(con)
    q = """--sql
CREATE OR REPLACE MACRO point_to_grid_id(point, x_min, y_min, x_max, y_max, cell_width, cell_height) AS (
    SELECT coord_to_grid_id(st_x(point), st_y(point), x_min, y_min, x_max, y_max, cell_width, cell_height)
);
"""
    con.execute(q)


def create_grid_id_to_coord(con: DuckDBPyConnection):
    q = """--sql
CREATE OR REPLACE MACRO grid_id_to_coord(id, x_min, y_min, x_max, y_max, cell_width, cell_height) AS (
    WITH variable AS (
        SELECT
            floor((x_max - x_min) / cell_width)   AS col_cnt,
            floor((y_max - y_min) / cell_height)  AS row_cnt
    )
    SELECT
        CASE
            WHEN id between 0 and (col_cnt * row_cnt) - 1 THEN
                {'x': id % col_cnt * cell_width + x_min, 'y': floor(id / col_cnt) * cell_height + y_min}
            ELSE {'x': NULL, 'y': NULL}
        END
    FROM variable
);
"""
    con.execute(q)


def create_grid_id_to_envelope(con: DuckDBPyConnection):
    create_grid_id_to_coord(con)

    q = """--sql
CREATE OR REPLACE MACRO grid_id_to_envelope(id, x_min, y_min, x_max, y_max, cell_width, cell_height) AS (
    WITH variable AS (
        SELECT grid_id_to_coord(id, x_min, y_min, x_max, y_max, cell_width, cell_height) AS coord
    )
    SELECT
        CASE
            WHEN coord.x is null or coord.y is null THEN
                ST_GeomFromText('POLYGON EMPTY')
            ELSE
                ST_MakeEnvelope(coord.x, coord.y, coord.x + cell_width, coord.y + cell_height)
        END
    FROM variable
);
"""
    con.execute(q)


def create_gchain_trim_key(con: DuckDBPyConnection):
    q = """--sql
CREATE OR REPLACE MACRO gchain_trim_key(id, level) AS (
    SELECT cast(cast(id AS VARCHAR)[0:level*2+1] AS BIGINT)
);
"""
    con.execute(q)
