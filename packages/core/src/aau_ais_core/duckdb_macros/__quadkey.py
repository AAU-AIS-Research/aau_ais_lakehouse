from duckdb import DuckDBPyConnection


def create_quadkey_bit_encode(con: DuckDBPyConnection):
    q = """--sql
create or replace macro quadkey_bit_encode(qkey) as (
    with expanded as (
        select unnest(string_split(qkey, '')) as digit
    ), mapped as (
        select case digit
            when '0' then '00'
            when '1' then '01'
            when '2' then '10'
            when '3' then '11'
            end as bits
        from expanded
    )
    select string_agg(bits, '')::bit as qkey
    from mapped
);
"""
    con.execute(q)


def create_quadkey_int16_encode(con: DuckDBPyConnection):
    create_quadkey_bit_encode(con)

    q = """--sql
create or replace macro quadkey_int16_encode(qkey) as (
    select quadkey_bit_encode(qkey)::int16 as qkey
);
"""
    con.execute(q)


def create_quadkey_int32_encode(con: DuckDBPyConnection):
    create_quadkey_bit_encode(con)

    q = """--sql
create or replace macro quadkey_int32_encode(qkey) as (
    select quadkey_bit_encode(qkey)::int32 as qkey
);
"""
    con.execute(q)


def create_quadkey_int64_encode(con: DuckDBPyConnection):
    create_quadkey_bit_encode(con)

    q = """--sql
create or replace macro quadkey_int64_encode(qkey) as (
    select quadkey_bit_encode(qkey)::int64 as qkey
);
"""
    con.execute(q)


def create_quadkey_to_zxy(con: DuckDBPyConnection):
    q = """--sql
create or replace macro quadkey_to_zxy(qkey) as table (
    WITH RECURSIVE
    digits(qkey) AS (
        SELECT
            qkey,
            length(qkey)::int           AS z,
            pos + 1                     AS pos,
            substring(qkey, pos + 1, 1) AS digit
        FROM range(length(qkey))        AS t(pos)
    ),
    recur(pos, digit, z, x, y) AS (
        -- base case
        SELECT 0, NULL, (SELECT z FROM digits LIMIT 1), 0::INT, 0::INT
        UNION ALL
        -- recursive step
        SELECT
            d.pos,
            d.digit,
            r.z,
            r.x * 2 + CASE d.digit WHEN '1' THEN 1 WHEN '3' THEN 1 ELSE 0 END,
            r.y * 2 + CASE d.digit WHEN '2' THEN 1 WHEN '3' THEN 1 ELSE 0 END
        FROM recur r
        JOIN digits d ON d.pos = r.pos + 1
    )
    SELECT
        z,
        x,
        y
    FROM recur
    WHERE pos = z
);
"""
    con.execute(q)


def create_zxy_to_quadkey(con: DuckDBPyConnection):
    q = """--sql
create or replace macro zxy_to_quadkey(z, x, y) as (
    WITH RECURSIVE bits(level, quad) AS (
        -- start at level = z, quad = empty string
        SELECT z AS level, ''::VARCHAR AS quad
        UNION ALL -- at each step compute digit for current level and append then decrement level
        SELECT
            level - 1 AS level,
            quad || CAST(
            (
                CAST(floor(x / pow(2, level-1)) AS BIGINT) % 2 -- bit for x at this level (0 or 1)
            +
                2 * (CAST(floor(y / pow(2, level-1)) AS BIGINT) % 2) -- bit for y times 2
            ) AS INTEGER
            ) AS quad
        FROM bits
        WHERE level > 0
    )
    -- when recursion finishes, one row will have level = 0 and quad = full quadkey
    SELECT quad FROM bits WHERE level = 0 LIMIT 1
);
"""
    con.execute(q)


def create_int_to_quadkey(con: DuckDBPyConnection):
    q = """--sql
create or replace macro int_to_quadkey(val, z) as (
    with input as (
        select right(val::bit::varchar, z * 2) as bitstring
    )
    SELECT string_agg(digit, '' order by pos) AS quadkey
    FROM (
    SELECT
        CASE substr(bitstring, pos, 2)
        WHEN '00' THEN '0' WHEN '01' THEN '1' WHEN '10' THEN '2' WHEN '11' THEN '3'
        END AS digit,
        pos
    FROM input, generate_series(1, length(bitstring), 2) AS gs(pos)
    ORDER BY pos
    ) t
);
"""
    con.execute(q)
