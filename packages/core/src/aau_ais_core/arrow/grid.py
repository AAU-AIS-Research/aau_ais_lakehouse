# type: ignore
import math

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import (
    ChunkedArray,
    DoubleArray,
    DoubleScalar,
    Int64Array,
    Int64Scalar,
    Scalar,
    Table,
    float64,
    int8,
    int64,
    scalar,
)
from pyarrow.compute import CastOptions


def modulo(
    dividend,
    divisor,
):
    return pc.subtract(
        dividend,
        pc.multiply(divisor, pc.floor(pc.divide(dividend, divisor))),  # type: ignore
    )


def gid_to_coord(
    id: ChunkedArray | Int64Array | Int64Scalar,
    x_min: DoubleScalar | Int64Scalar | DoubleArray | Int64Array,
    y_min: DoubleScalar | Int64Scalar | DoubleArray | Int64Array,
    x_max: DoubleScalar | Int64Scalar | DoubleArray | Int64Array,
    y_max: DoubleScalar | Int64Scalar | DoubleArray | Int64Array,
    cell_width: DoubleScalar | Int64Scalar | DoubleArray | Int64Array,
    cell_height: DoubleScalar | Int64Scalar | DoubleArray | Int64Array,
) -> Table:
    if isinstance(id, ChunkedArray):
        id = id.combine_chunks()
    elif isinstance(id, Int64Scalar):
        id = pa.array([id])
    elif not isinstance(id, Int64Array):
        raise TypeError(
            "id must be a ChunkedArray, Int64Array, or Int64Scalar not %s" % type(id)
        )

    width = pc.subtract(x_max, x_min)
    height = pc.subtract(y_max, y_min)

    cast_options = CastOptions(
        target_type=int64(),
        allow_float_truncate=True,
    )

    col_cnt = pc.divide(width, cell_width).cast(options=cast_options)
    row_cnt = pc.divide(height, cell_height).cast(options=cast_options)

    x = pc.add(pc.multiply(modulo(id, col_cnt), cell_width), x_min)
    y = pc.add(pc.multiply(pc.floor(pc.divide(id, col_cnt)), cell_height), y_min)

    upper_bound = pc.subtract(pc.multiply(col_cnt, row_cnt), 1)
    condition = pc.and_(pc.greater_equal(id, 0), pc.less_equal(id, upper_bound))

    return Table.from_arrays(
        [id, pc.if_else(condition, x, None), pc.if_else(condition, y, None)],
        ["id", "x", "y"],
    )


def coord_to_gid(
    x: ChunkedArray,
    y: ChunkedArray,
    x_min: DoubleScalar | Int64Scalar | DoubleArray | Int64Array,
    y_min: DoubleScalar | Int64Scalar | DoubleArray | Int64Array,
    x_max: DoubleScalar | Int64Scalar | DoubleArray | Int64Array,
    y_max: DoubleScalar | Int64Scalar | DoubleArray | Int64Array,
    cell_width: DoubleScalar | Int64Scalar | DoubleArray | Int64Array,
    cell_height: DoubleScalar | Int64Scalar | DoubleArray | Int64Array,
):
    width = pc.subtract(x_max, x_min)
    height = pc.subtract(y_max, y_min)
    col_cnt = pc.floor(pc.divide(width, cell_width))  # type: ignore
    row_cnt = pc.floor(pc.divide(height, cell_height))  # type: ignore
    i = pc.floor(pc.divide(pc.subtract(x, x_min), cell_width))  # type: ignore
    j = pc.floor(pc.divide(pc.subtract(y, y_min), cell_height))  # type: ignore
    x = pc.if_else(pc.is_finite(x), x, None)
    y = pc.if_else(pc.is_finite(y), y, None)

    conditions = [
        pc.greater(scalar(0), i),
        pc.greater_equal(i, col_cnt),
        pc.greater(scalar(0), j),
        pc.greater_equal(j, row_cnt),
        pc.is_inf(x),
        pc.is_inf(y),
    ]

    condition = pc.or_(conditions[0], conditions[1])
    for cond in conditions[2:]:
        condition = pc.or_(condition, cond)

    results = pc.if_else(
        condition,
        scalar(-1),
        pc.add(pc.multiply(col_cnt, j), i),
    )

    return pc.cast(results, int64())


def to_gchain_key(
    x: ChunkedArray,
    y: ChunkedArray,
    x_min: DoubleScalar | Int64Scalar,
    y_min: DoubleScalar | Int64Scalar,
    x_max: DoubleScalar | Int64Scalar,
    y_max: DoubleScalar | Int64Scalar,
    base_size: DoubleScalar | Int64Scalar,
) -> Int64Array:
    exponent = math.log10(base_size.as_py()) * 2 - 2
    idx_base = math.pow(10, exponent)
    rv = pa.repeat(scalar(idx_base), len(x))
    i = 0

    while not pc.equal(base_size, scalar(1.0)).as_py():
        ids = coord_to_gid(x, y, x_min, y_min, x_max, y_max, base_size, base_size)
        coords: Table = gid_to_coord(
            ids, x_min, y_min, x_max, y_max, base_size, base_size
        )

        x_min = coords.column("x")
        y_min = coords.column("y")
        x_max = pc.add(x_min, base_size)
        y_max = pc.add(y_min, base_size)

        base_size = pc.divide(base_size, scalar(10))

        if i != 0:
            exponent -= 2
            factor = scalar(math.pow(10, exponent))
            rv = pc.add(pc.multiply(ids, factor), rv)
        i += 1

    rv = pc.if_else(pc.is_null(rv), scalar(-1), rv)
    return rv.cast(int64())  # type: ignore


def segment_gchain_key(key: Int64Array | Int64Scalar) -> Table:
    """
    Splits a base-10 id into its hierarchical segments.

    For example, an id 1336884 will be split into segments [33, 68, 84].

    :param base10_id: A base-10 id to split
    :type base10_id: Int64Array | Int64Scalar
    :return: A Table with a single column per segment
    :rtype: Table
    """
    key = pa.array([key]) if isinstance(key, Scalar) else key
    tbl = Table.from_arrays([key], ["id"])

    exponent = pc.floor(pc.log10(key))
    unique_exponents = pc.unique(exponent)
    if len(unique_exponents) > 1:
        raise ValueError("All suplied ids must share the same exponent!")

    exponent: int = pc.cast(unique_exponents[0], int8()).as_py()
    key = pc.cast(modulo(key, pc.power(10, exponent)), int64())

    for i, exp_step in enumerate(range(exponent, 0, -2), 1):
        divisor = pc.power(10, pc.subtract(exp_step, 2))

        # Remove previous id section e.g.
        # 336884 % 10^6 => 336884
        # 336884 % 10^4 => 6884
        # 336884 % 10^2 => 84
        trim = modulo(key, pc.power(10, exp_step))

        # Remove subsequent id section(s), e.g.
        # 336884 // 10^4 => 33
        # 6884 // 10^2 => 68
        # 84 // 10^0 => 84
        segment = pc.cast(
            pc.divide(trim, divisor),
            options=CastOptions(
                target_type=int8(),
                allow_float_truncate=True,
            ),
        )

        tbl = tbl.add_column(i, str(i), segment)

    return tbl


def gchain_key_to_bounds(
    key: Int64Array | ChunkedArray,
    ref_id: Int64Array | ChunkedArray,
    ref_x_min: DoubleScalar | Int64Scalar,
    ref_y_min: DoubleScalar | Int64Scalar,
    ref_x_max: DoubleScalar | Int64Scalar,
    ref_y_max: DoubleScalar | Int64Scalar,
    ref_size: DoubleScalar | Int64Scalar,
) -> Table:
    if isinstance(ref_x_min, (int, float)):
        ref_x_min = scalar(ref_x_min)
    if isinstance(ref_y_min, (int, float)):
        ref_y_min = scalar(ref_y_min)
    if isinstance(ref_x_max, (int, float)):
        ref_x_max = scalar(ref_x_max)
    if isinstance(ref_y_max, (int, float)):
        ref_y_max = scalar(ref_y_max)
    if isinstance(ref_size, (int, float)):
        ref_size = scalar(ref_size)

    levels = pc.unique(
        pc.divide(pc.log10(key), 2).cast(
            options=CastOptions(
                target_type=int8(),
                allow_float_truncate=True,
            )
        )
    )
    if len(levels) == 0:
        schema = pa.schema(
            [
                ("ref_id", int64()),
                ("gchain_key", int64()),
                ("x_min", float64()),
                ("y_min", float64()),
                ("x_max", float64()),
                ("y_max", float64()),
            ]
        )
        return Table.from_arrays(
            [
                pa.array([], type=int64()),
                pa.array([], type=int64()),
                pa.array([], type=float64()),
                pa.array([], type=float64()),
                pa.array([], type=float64()),
                pa.array([], type=float64()),
            ],
            schema=schema,
        )
    if len(levels) > 1:
        raise ValueError("All keys must have the same depth")
    max_level = levels[0].as_py()

    segments = segment_gchain_key(key)

    base_coord = gid_to_coord(
        ref_id, ref_x_min, ref_y_min, ref_x_max, ref_y_max, ref_size, ref_size
    )
    x_min = base_coord.column("x")
    y_min = base_coord.column("y")
    x_max = pc.add(x_min, ref_size)
    y_max = pc.add(y_min, ref_size)

    current_level = 1
    while current_level <= max_level:
        lvl_ids = segments.column(str(current_level))

        cell_size = ref_size.as_py() / math.pow(10, current_level)
        coords = gid_to_coord(lvl_ids, x_min, y_min, x_max, y_max, cell_size, cell_size)
        x_min = coords.column("x")
        y_min = coords.column("y")
        x_max = pc.add(coords.column("x"), cell_size)
        y_max = pc.add(coords.column("y"), cell_size)
        current_level += 1

    return Table.from_arrays(
        [ref_id, key, x_min, y_min, x_max, y_max],
        ["ref_id", "gchain_key", "x_min", "y_min", "x_max", "y_max"],
    )


# tbl = segment_base10_id(scalar(1336884))
# duckdb.from_arrow(tbl).show()
# tbl = segment_gchain_key(pa.array([100]))
# duckdb.from_arrow(tbl).show()

# tbl = gchain_key_to_bounds(
#     pa.array([16884]),
#     pa.array([17692581]),
#     scalar(1599625),
#     scalar(762675),
#     scalar(6567875),
#     scalar(6743925),
#     scalar(1000),
# )
# duckdb.from_arrow(tbl).show()
