import pytest

from aau_ais_core import quadkey_utils


@pytest.mark.parametrize(
    "lon,lat,zoom,expected",
    [
        (10.046, 56.054, 15, (17298, 10194)),
        (10.467, 56.237, 15, (17336, 10165)),
    ],
)
def test_get_zxy(lon: float, lat: float, zoom: int, expected: tuple[int, int]):
    z, x, y = quadkey_utils.get_zxy(lon, lat, zoom)

    assert z == zoom
    assert (x, y) == expected


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
def test_int_to_quadkey(int_key: int, lvl: int, expected: str):
    result = quadkey_utils.int_to_quadkey(int_key, lvl)
    assert len(result) == lvl
    assert result == expected
