import math


def get_zxy(lon: float, lat: float, zoom: int) -> tuple[int, int, int]:
    """
    Converts latitude, longitude, and zoom level into tile coordinates (z, x, y).

    :param lat: Latitude in degrees.
    :param lon: Longitude in degrees.
    :param zoom: Zoom level.
    :return: A tuple of (zoom, x, y).
    """
    n = 2.0**zoom
    xtile = int((lon + 180.0) / 360.0 * n)

    # Convert latitude to radians
    lat_rad = math.radians(lat)
    ytile = int(
        (1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi)
        / 2.0
        * n
    )

    return (zoom, xtile, ytile)


def zxy_to_quadkey(z: int, x: int, y: int) -> str:
    qkey = ""

    for i in range(z, 0, -1):
        digit: int = 0
        mask: int = 1 << (i - 1)

        if x & mask != 0:
            digit += 1
        if y & mask != 0:
            digit += 2

        qkey += str(digit)

    return qkey


def quadkey_to_int(qkey: str) -> int:
    rv = 0

    for i, char in enumerate(qkey):
        if i != 0:
            rv = rv << 2
        rv = rv | int(char)
    return rv


def int_to_quadkey(value: int, zoom: int) -> str:
    byte_cnt = math.ceil(zoom / 4)
    test = value.to_bytes(byte_cnt, "big", signed=True)

    qkey = ""
    for byte in test:
        for i in range(6, -1, -2):  # Iterates through bit pairs in each byte
            # Shift the bit to the far right, then mask with 3 (11) to get its value (00, 01, 10, or 11)
            bit_pair = (byte >> i) & 3
            qkey += str(bit_pair)

    return qkey


def compute_qkey_range(qkey: str, partition_depth: int = 8) -> tuple[int, int]:
    qkey_depth = len(qkey)

    if qkey_depth >= partition_depth:
        part_key = quadkey_to_int(qkey[0:8])
        return (part_key, part_key)

    diff = partition_depth - qkey_depth

    a = qkey + "0" * diff
    b = qkey + "3" * diff

    sfrom = quadkey_to_int(a)
    sto = quadkey_to_int(b)

    return (sfrom, sto)
