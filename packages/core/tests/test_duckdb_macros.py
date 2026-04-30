import duckdb
import pytest
from duckdb import DuckDBPyConnection

from aau_ais_core import duckdb_macros


@pytest.fixture
def con():
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    duckdb_macros.create_coord_to_grid_id(con)
    duckdb_macros.create_wgs84_coord_to_grid_id(con)
    duckdb_macros.create_grid_id_to_coord(con)
    duckdb_macros.create_gchain_trim_key(con)
    duckdb_macros.create_int_to_quadkey(con)
    duckdb_macros.create_quadkey_to_zxy(con)

    return con


@pytest.mark.parametrize(
    "x,y,x_min,y_min,x_max,y_max,cell_width,cell_height,expected",
    [
        (0, 0, 0, 0, 15, 15, 1, 1, 0),
        (15, 15, 0, 0, 16, 16, 1, 1, 255),
        (15, 0, 0, 0, 15, 15, 1, 1, -1),
        (16, 1, 1, 1, 17, 17, 1, 1, 15),
        (3, 0, 0, 0, 4, 4, 0.5, 0.5, 6),
        (3.49, 0, 0, 0, 4, 4, 0.5, 0.5, 6),
        (3.5, 0, 0, 0, 4, 4, 0.5, 0.5, 7),
        (4, 0, 0, 0, 4, 4, 0.5, 0.5, -1),
        (180, 0, 0, 0, 180, 90, 0.5, 0.5, -1),
        (11.477506637573242, 57.86309814453125, 0, 0, 180, 90, 0.5, 0.5, 41422),
        (16.4549617767334, 56.233604431152344, 0, 0, 180, 90, 0.5, 0.5, 40352),
    ],
)
def test_coord_to_grid_id(
    con: DuckDBPyConnection,
    x: float,
    y: float,
    x_min: float,
    y_min: float,
    x_max: float,
    y_max: float,
    cell_width: float,
    cell_height: float,
    expected: int,
):
    with con:
        rows = con.query(
            "SELECT coord_to_grid_id(?, ?, ?, ?, ?, ?, ?, ?)",
            params=[x, y, x_min, y_min, x_max, y_max, cell_width, cell_height],
        ).fetchall()

        assert len(rows) == 1
        columns = rows[0]
        assert len(columns) == 1

        result = columns[0]
        assert result == expected


@pytest.mark.parametrize(
    "lon,lat,cell_width,cell_height,expected",
    [
        (180, 0, 0.5, 0.5, 200000),
        (179.9, 0, 0.5, 0.5, 100359),
        (11.477506637573242, 57.86309814453125, 0.5, 0.5, 141422),
        (16.4549617767334, 56.233604431152344, 0.5, 0.5, 140352),
        (-72.37855529785156, 46.40056610107422, 0.5, 0.5, 233335),
        (-3.7664780616760254, 58.71392822265625, 0.5, 0.5, 242472),
        (-165.11976623535156, -66.2571792602539, 0.5, 0.5, 316949),
        (14.283212661743164, -84.89945220947266, 0.5, 0.5, 403628),
    ],
)
def test_wgs84_coord_to_grid_id(
    con: DuckDBPyConnection,
    lon: float,
    lat: float,
    cell_width: float,
    cell_height: float,
    expected: int,
):
    with con:
        rows = con.query(
            "SELECT wgs84_coord_to_grid_id(?, ?, ?, ?)",
            params=[lon, lat, cell_width, cell_height],
        ).fetchall()

        assert len(rows) == 1
        columns = rows[0]
        assert len(columns) == 1

        result = columns[0]
        assert result == expected


@pytest.mark.parametrize(
    "grid_id,x_min,y_min,x_max,y_max,cell_width,cell_height,expected",
    [
        (
            16411067,
            1599625,
            762675,
            6567875,
            6743925,
            1000,
            1000,
            {"x": 3362625, "y": 4065675},
        ),
        (
            17692581,
            1599625,
            762675,
            6567875,
            6743925,
            1000,
            1000,
            {"x": 3132625, "y": 4323675},
        ),
    ],
)
def test_grid_id_to_coord(
    con: DuckDBPyConnection,
    grid_id: int,
    x_min: float,
    y_min: float,
    x_max: float,
    y_max: float,
    cell_width: float,
    cell_height: float,
    expected: dict[str, int],
):
    with con:
        rows = con.query(
            "SELECT grid_id_to_coord(?, ?, ?, ?, ?, ?, ?)",
            params=[grid_id, x_min, y_min, x_max, y_max, cell_width, cell_height],
        ).fetchall()

        assert len(rows) == 1
        columns = rows[0]
        assert len(columns) == 1
        result = columns[0]

        assert result["x"] == expected["x"]
        assert result["y"] == expected["y"]


@pytest.mark.parametrize(
    "gchain_key,lvl,expected",
    [
        (
            1112233,
            2,
            11122,
        ),
        (
            1010203,
            2,
            10102,
        ),
        (
            1010203,
            1,
            101,
        ),
        (
            1010203,
            3,
            1010203,
        ),
        (
            10102030405060708,
            5,
            10102030405,
        ),
    ],
)
def test_gchain_trim_key(
    con: DuckDBPyConnection, gchain_key: int, lvl: int, expected: int
):
    with con:
        rows = con.query(
            "SELECT gchain_trim_key(?, ?)",
            params=[gchain_key, lvl],
        ).fetchall()

        assert len(rows) == 1
        columns = rows[0]
        assert len(columns) == 1
        result = columns[0]

        assert result == expected


@pytest.mark.parametrize(
    "int_key,lvl,expected",
    [
        (-14146, 8, "30202332"),
        (-14148, 8, "30202330"),
        (14547, 8, "03203103"),
        (13805, 8, "03113231"),
        (25847, 8, "12103313"),
        (16384, 8, "10000000"),
        (14197, 8, "03131311"),
        (-13821, 8, "30220003"),
        (-13792, 8, "30220200"),
        (13808, 8, "03113300"),
    ],
)
def test_int_to_quadkey(con: DuckDBPyConnection, int_key: int, lvl: int, expected: str):
    with con:
        rows = con.query(
            "SELECT int_to_quadkey(?, ?)",
            params=[int_key, lvl],
        ).fetchall()

        assert len(rows) == 1
        columns = rows[0]
        assert len(columns) == 1
        result = columns[0]
        assert len(result) == lvl

        assert result == expected


@pytest.mark.parametrize(
    "quadkey,expected",
    [
        ("12020110022002023100", (20, 548876, 329304)),
        ("12002330212211211001", (20, 550105, 322336)),
    ],
)
def test_quadkey_to_zxy(
    con: DuckDBPyConnection, quadkey: str, expected: tuple[int, int, int]
):
    with con:
        rows = con.query(
            "select z, x, y from  quadkey_to_zxy(?)",
            params=[quadkey],
        ).fetchall()

        assert len(rows) == 1
        result = rows[0]
        assert len(result) == 3

        assert result == expected
