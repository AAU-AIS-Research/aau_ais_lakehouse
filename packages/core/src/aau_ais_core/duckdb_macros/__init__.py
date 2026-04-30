from duckdb import DuckDBPyConnection

from .__grid import (
    create_coord_to_grid_id,
    create_gchain_trim_key,
    create_grid_id_to_coord,
    create_grid_id_to_envelope,
    create_point_to_grid_id,
)
from .__quadkey import (
    create_int_to_quadkey,
    create_quadkey_bit_encode,
    create_quadkey_int16_encode,
    create_quadkey_int32_encode,
    create_quadkey_int64_encode,
    create_quadkey_to_zxy,
    create_zxy_to_quadkey,
)
from .__wgs84_grid import (
    create_coord_to_quadrant,
    create_grid_id_to_wgs84_coord,
    create_grid_id_to_wgs84_envelope,
    create_wgs84_coord_to_grid_id,
)


def create_minutes_since_midnight(con: DuckDBPyConnection):
    con.execute("""--sql
CREATE OR REPLACE MACRO minutes_since_midnight(ts) AS
    hour(ts::TIMESTAMP) * 60 + minute(ts::TIMESTAMP);
""")


def create_is_valid_mmsi(con: DuckDBPyConnection):
    con.execute(
        """--sql
CREATE OR REPLACE MACRO is_valid_mmsi(mmsi) AS length(mmsi::TEXT) <= 9;
"""
    )


def create_mmsi_to_radio_service_type(con: DuckDBPyConnection):
    con.execute(
        """--sql
CREATE OR REPLACE MACRO mmsi_to_radio_service_type(mmsi) AS (
    WITH cte AS (
        SELECT lpad(mmsi::varchar, 9, '0') AS mmsi_str
    )
    SELECT CASE
        WHEN left(mmsi_str, 2) = '00' THEN
            CASE
                WHEN substring(mmsi_str, 6, 1) = '1' THEN 'cost station'
                WHEN substring(mmsi_str, 6, 1) = '2' THEN 'port station'
                WHEN substring(mmsi_str, 6, 1) = '3' THEN 'pilot station'
                WHEN substring(mmsi_str, 6, 1) = '4' THEN 'ais repeater station'
                WHEN substring(mmsi_str, 6, 1) = '5' THEN 'ais base station'
                ELSE 'unknown'
            END
        WHEN left(mmsi_str, 1) = '0' THEN 'ship group'
        WHEN left(mmsi_str, 1)::SMALLINT BETWEEN 2 AND 7 THEN 'ship'
        WHEN left(mmsi_str, 3) = '111' THEN
            CASE
                WHEN substring(mmsi_str, 7, 1) = '1' THEN 'fixed-wing aircraft'
                WHEN substring(mmsi_str, 7, 1) = '5' THEN 'helicopter'
                ELSE 'unknown'
            END
        WHEN left(mmsi_str, 2) = '99' THEN
            CASE
                WHEN substring(mmsi_str, 6, 1) = '1' THEN 'physical ais aton'
                WHEN substring(mmsi_str, 6, 1) = '6' THEN 'virtual ais aton'
                WHEN substring(mmsi_str, 6, 1) = '8' THEN 'mobile aton'
                ELSE 'unknown'
            END
        WHEN left(mmsi_str, 2) = '98' THEN 'deployable child craft'
        WHEN left(mmsi_str, 1) = '8' THEN 'handheld vhf transceiver with dsc and integral gnss receiver'
        WHEN left(mmsi_str, 3) = '970' THEN 'ais-sart'
        WHEN left(mmsi_str, 3) = '972' THEN 'man overboard'
        WHEN left(mmsi_str, 3) = '974' THEN 'epirb-ais'
        WHEN left(mmsi_str, 3) = '979' THEN 'amrd group b'
        ELSE 'unknown'
    END
    FROM cte
);
"""
    )


def create_mmsi_to_mid(con: DuckDBPyConnection):
    con.execute(
        """--sql
CREATE OR REPLACE MACRO mmsi_to_mid(mmsi) AS (
    WITH cte AS (
        SELECT lpad(mmsi::varchar, 9, '0') AS mmsi_str
    )
    SELECT CASE
        WHEN left(mmsi_str, 2) IN ('00', '98', '99') THEN substring(mmsi_str, 3, 3)::SMALLINT
        WHEN left(mmsi_str, 1) IN ('0', '8') THEN substring(mmsi_str, 2, 3)::SMALLINT
        WHEN left(mmsi_str, 3) = '111' THEN substring(mmsi_str, 4, 3)::SMALLINT
        WHEN left(mmsi_str, 1)::SMALLINT BETWEEN 2 AND 7 THEN substring(mmsi_str, 1, 3)::SMALLINT
        ELSE NULL
    END
    FROM cte
);
"""
    )


def create_is_valid_imo(con: DuckDBPyConnection):
    con.execute("""--sql
CREATE OR REPLACE MACRO imo_checksum(imo) AS TABLE (
    WITH imo_digits AS (
        SELECT substring(imo::TEXT, 1, 1)::SMALLINT AS d1,
               substring(imo::TEXT, 2, 1)::SMALLINT AS d2,
               substring(imo::TEXT, 3, 1)::SMALLINT AS d3,
               substring(imo::TEXT, 4, 1)::SMALLINT AS d4,
               substring(imo::TEXT, 5, 1)::SMALLINT AS d5,
               substring(imo::TEXT, 6, 1)::SMALLINT AS d6,
               substring(imo::TEXT, 7, 1)::SMALLINT AS d7
    )
    SELECT d7                                                                                           AS check_digit,
           right(((d1 * 7) + (d2 * 6) + (d3 * 5) + (d4 * 4) + (d5 * 3) + (d6 * 2))::TEXT, 1)::SMALLINT  AS comp_check_digit
    FROM imo_digits
    WHERE imo IS NOT NULL AND imo >= 1000000 AND imo <= 9999999
);


CREATE OR REPLACE MACRO is_valid_imo(imo) AS (
    SELECT  CASE
                WHEN imo IS NULL OR imo < 1000000 OR imo > 9999999 THEN FALSE
                ELSE (SELECT check_digit = comp_check_digit FROM imo_checksum(imo))
            END
);""")
