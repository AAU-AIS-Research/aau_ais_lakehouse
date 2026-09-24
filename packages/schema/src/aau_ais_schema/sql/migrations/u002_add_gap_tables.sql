--#region Views  (drop first: depends on fact + dim tables)
DROP VIEW IF EXISTS lakehouse.main.vessel_activity;
--#endregion

--#region Facts
DROP TABLE IF EXISTS lakehouse.fact.ais_point_fact;   -- also drops its partitions
DROP TABLE IF EXISTS lakehouse.fact.ais_traj_fact;
DROP TABLE IF EXISTS lakehouse.fact.ais_stop_fact;
--#endregion

--#region Dimensions  (drop tables before their sequences)
DROP TABLE IF EXISTS dim.load_dim;
DROP TABLE IF EXISTS dim.date_dim;
DROP TABLE IF EXISTS dim.time_dim;
DROP TABLE IF EXISTS dim.country_dim;
DROP TABLE IF EXISTS dim.vessel_dim;
DROP TABLE IF EXISTS dim.vessel_config_dim;
DROP TABLE IF EXISTS dim.depth_grid_dim;
DROP TABLE IF EXISTS dim.half_degree_grid_dim;
DROP TABLE IF EXISTS dim.transponder_type_dim;
DROP TABLE IF EXISTS dim.vessel_type_dim;
DROP TABLE IF EXISTS dim.vessel_name_dim;
DROP TABLE IF EXISTS dim.pos_type_dim;
DROP TABLE IF EXISTS dim.cargo_type_dim;
DROP TABLE IF EXISTS dim.call_sign_dim;
DROP TABLE IF EXISTS dim.destination_dim;
DROP TABLE IF EXISTS dim.nav_status_dim;
DROP TABLE IF EXISTS dim.traj_type_dim;
DROP TABLE IF EXISTS dim.traj_state_change_dim;
DROP TABLE IF EXISTS dim.traj_geom_dim;
DROP TABLE IF EXISTS dim.stop_geom_dim;
--#endregion

--#region Sequences  (safe now that the dependent tables are gone; no CASCADE)
DROP SEQUENCE IF EXISTS dim.load_dim_load_id_seq;
DROP SEQUENCE IF EXISTS dim.vessel_dim_vessel_id_seq;
DROP SEQUENCE IF EXISTS dim.vessel_config_dim_vessel_config_id_seq;
DROP SEQUENCE IF EXISTS dim.depth_grid_dim_depth_cell_id_seq;
DROP SEQUENCE IF EXISTS dim.half_degree_grid_dim_half_degree_cell_id_seq;
DROP SEQUENCE IF EXISTS dim.transponder_type_dim_transponder_type_id_seq;
DROP SEQUENCE IF EXISTS dim.vessel_type_dim_vessel_type_id_seq;
DROP SEQUENCE IF EXISTS dim.vessel_name_dim_vessel_name_id_seq;
DROP SEQUENCE IF EXISTS dim.pos_type_dim_pos_type_id_seq;
DROP SEQUENCE IF EXISTS dim.cargo_type_dim_cargo_type_id_seq;
DROP SEQUENCE IF EXISTS dim.call_sign_dim_call_sign_id_seq;
DROP SEQUENCE IF EXISTS dim.destination_dim_destination_id_seq;
DROP SEQUENCE IF EXISTS dim.nav_status_dim_nav_status_id_seq;
DROP SEQUENCE IF EXISTS dim.traj_type_dim_traj_type_id_seq;
DROP SEQUENCE IF EXISTS dim.traj_state_change_dim_state_change_id_seq;
DROP SEQUENCE IF EXISTS dim.traj_geom_dim_geom_id_seq;
DROP SEQUENCE IF EXISTS dim.stop_geom_dim_geom_id_seq;
--#endregion

--#region Macros
DROP MACRO IF EXISTS minutes_since_midnight;
DROP MACRO IF EXISTS quadkey_bit_encode;
DROP MACRO IF EXISTS quadkey_uint16_encode;
DROP MACRO IF EXISTS quadkey_uint32_encode;
DROP MACRO IF EXISTS quadkey_uint64_encode;
DROP MACRO IF EXISTS quadkey_int16_encode;
DROP MACRO IF EXISTS quadkey_int32_encode;
DROP MACRO IF EXISTS quadkey_int64_encode;
DROP MACRO IF EXISTS quadkey_to_zxy;
DROP MACRO IF EXISTS int_to_quadkey;
DROP MACRO IF EXISTS coord_to_quadrant;
DROP MACRO IF EXISTS coord_to_grid_id;
DROP MACRO IF EXISTS wgs84_coord_to_grid_id;
DROP MACRO IF EXISTS grid_id_to_coord;
DROP MACRO IF EXISTS grid_id_to_wgs84_coord;
DROP MACRO IF EXISTS grid_id_to_wgs84_envelope;
DROP MACRO IF EXISTS datetime_keys_to_timestamp;
DROP MACRO IF EXISTS ts_to_temporal_part_key;
--#endregion

--#region Schemas  (last; empty now)
DROP SCHEMA IF EXISTS dim;
DROP SCHEMA IF EXISTS lakehouse.fact;
--#endregion