--------------------------------------------------------------------------------------
-- Schema
--------------------------------------------------------------------------------------
create schema if not exists dim;
create schema if not exists lakehouse.fact;

-- Load dimension
create table if not exists dim.load_dim(
    load_id         uinteger        primary key,
    src_id          varchar(100)    not null,
    dst_tbl         varchar(100)    not null,
    start_ts        timestamp       not null,
    end_ts          timestamp,
    total_duration  interval,
    no_rows         ubigint,
    ingest_start_ts timestamp,
    ingest_end_ts   timestamp,
    ingest_duration interval,
    ingest_per_sec  float,
    failed          boolean,
    deleted         boolean
);

-- Date dimension
-- date_id is a smart key i.e. YYYYMMDD
create table if not exists dim.date_dim (
    date_id    uinteger     primary key,
    year_no    usmallint    not null,
    month_no   utinyint     not null,
    day_no     utinyint     not null,
    week_no    utinyint     not null,
    weekday_no utinyint     not null,
    quarter_no utinyint     not null,
    iso_date   VARCHAR(10)  not null,
    month_name VARCHAR(9)   not null
);

-- Time dimension
-- time_id is a smart key i.e. HHMMSS
create table if not exists dim.time_dim(
    time_id        uinteger     primary key,
    hour_no        utinyint     not null,
    minute_no      utinyint     not null,
    second_no      utinyint     not null,
    fifteen_min_no utinyint     not null,
    five_min_no    usmallint    not null
);

-- Country dimension
create table if not exists dim.country_dim(
    alpha2                   char(2)        primary key,
    alpha3                   char(3)        unique not null,
    country_name             varchar(56)    unique not null,
    country_code             usmallint      unique not null,
    region_code              usmallint,
    sub_region_code          usmallint,
    intermediate_region_code usmallint,
    region                   varchar(8),
    sub_region               varchar(31),
    intermediate_region      varchar(15)
);

-- Vessel dimension
create sequence if not exists dim.vessel_dim_vessel_id_seq;
create table if not exists dim.vessel_dim(
    vessel_id           uinteger    primary key default nextval('dim.vessel_dim_vessel_id_seq'),
    mmsi                uinteger    not null,
    imo                 uinteger,
    mid                 usmallint,
    radio_service_type  varchar     not null,
    is_valid_mmsi       boolean     not null,
    is_valid_imo        boolean     not null,
    in_eu_mrv_db        boolean     default null,
    unique (mmsi, imo)
);

-- Vessel configuration dimension
create sequence if not exists dim.vessel_config_dim_vessel_config_id_seq;
create table if not exists dim.vessel_config_dim(
    vessel_config_id    uinteger    primary key default nextval('dim.vessel_config_dim_vessel_config_id_seq'),
    length              float,
    width               float,
    height              float,
    max_draught         float,
    dwt                 float,
    grt                 float,
    to_bow              float,
    to_stern            float,
    to_port             float,
    to_starboard        float,
    main_engine_kwh     float,
    aux_engine_kwh      float,
    unique (length, width, height, max_draught, dwt, grt, to_bow, to_stern, to_port, to_starboard, main_engine_kwh, aux_engine_kwh)
);


-- Transponder type dimension
create sequence if not exists dim.transponder_type_dim_transponder_type_id_seq;
create table if not exists dim.transponder_type_dim(
    transponder_type_id usmallint   primary key default nextval('dim.transponder_type_dim_transponder_type_id_seq'),
    transponder_type    varchar(29) not null unique
);
insert or ignore into dim.transponder_type_dim(transponder_type) values ('unknown');

-- Vessel type dimension
create sequence if not exists dim.vessel_type_dim_vessel_type_id_seq;
create table if not exists dim.vessel_type_dim(
    vessel_type_id     usmallint    primary key default nextval('dim.vessel_type_dim_vessel_type_id_seq'),
    vessel_type        varchar(250) not null unique
);
insert or ignore into dim.vessel_type_dim(vessel_type) values ('unknown');

-- Vessel name dimension
create sequence if not exists dim.vessel_name_dim_vessel_name_id_seq;
create table if not exists dim.vessel_name_dim(
    vessel_name_id  uinteger        primary key default nextval('dim.vessel_name_dim_vessel_name_id_seq'),
    vessel_name     varchar(250)    not null unique
);
insert or ignore into dim.vessel_name_dim(vessel_name) values ('unknown');

create sequence if not exists dim.pos_type_dim_pos_type_id_seq;
create table if not exists dim.pos_type_dim(
    pos_type_id usmallint       primary key default nextval('dim.pos_type_dim_pos_type_id_seq'),
    pos_type    varchar(250)    not null unique
);

create sequence if not exists dim.cargo_type_dim_cargo_type_id_seq;
create table if not exists dim.cargo_type_dim(
    cargo_type_id   usmallint   primary key default nextval('dim.cargo_type_dim_cargo_type_id_seq'),
    cargo_type      varchar(50) not null unique
);

create sequence if not exists dim.call_sign_dim_call_sign_id_seq;
create table if not exists dim.call_sign_dim(
    call_sign_id    uinteger        primary key default nextval('dim.call_sign_dim_call_sign_id_seq'),
    call_sign       varchar(250)    not null unique
);

create sequence if not exists dim.destination_dim_destination_id_seq;
create table if not exists dim.destination_dim(
    destination_id  uinteger    primary key default nextval('dim.destination_dim_destination_id_seq'),
    org_msg         text        unique          not null
);

--------------------------------------------------------------------------------------
-- Trajectory
--------------------------------------------------------------------------------------
-- The categories of sub-parts of an AIS sequence from a vessel
create sequence if not exists dim.traj_type_dim_traj_type_id_seq;
create table if not exists dim.traj_type_dim(
    traj_type_id  usmallint   primary key default nextval('dim.traj_type_dim_traj_type_id_seq'),
    traj_type     varchar(10) not null unique
);
insert or ignore into dim.traj_type_dim(traj_type) values
    ('in motion'),
    ('stationary'),
    ('outlier');

create sequence if not exists dim.traj_state_change_dim_state_change_id_seq;
create table if not exists dim.traj_state_change_dim(
    state_change_id usmallint   primary key default nextval('dim.traj_state_change_dim_state_change_id_seq'),
    state_change    varchar(10) not null unique
);
insert or ignore into dim.traj_state_change_dim(state_change) values
    ('unknown'),
    ('initial'),
    ('temporal gap'),
    ('spatial gap'),
    ('speeding'),
    ('corrupt'),
    ('stopping'),
    ('starting');

-- Trajectory geometry dimension
create sequence if not exists dim.traj_geom_dim_geom_id_seq;
create table if not exists dim.traj_geom_dim(
    geom_id                 uinteger    primary key default nextval('dim.traj_geom_dim_geom_id_seq'),
    geom                    geometry    not null,           
    start_point             geometry    not null,           -- ST_PointN(geom, 1)    
    end_point               geometry    not null,           -- ST_PointN(geom, length(geom))    
    is_simple_geom          boolean     not null,           -- ST_IsSimple(geom)
    is_valid_geom           boolean     not null,           -- ST_IsValid(geom) 
    cust_simplified_geom_00 geometry,                       -- To populate later, extensible SQL, Oracle style
    cust_simplified_geom_01 geometry,                       -- To populate later
    cust_simplified_geom_02 geometry,                       -- To populate later
    cust_simplified_geom_03 geometry,                       -- To populate later
    cust_simplified_geom_04 geometry,                       -- To populate later
    check (ST_GeometryType(geom) = 'LINESTRING')
);


create table if not exists lakehouse.fact.ais_traj_fact(
    ais_traj_id             uinteger        not null,
    load_id                 uinteger        not null,
    state_change_id         usmallint       not null,
    start_date_id           uinteger        not null,
    start_time_id           uinteger        not null,
    end_date_id             uinteger        not null,
    end_time_id             uinteger        not null,
    transponder_type_id     usmallint       not null,
    vessel_id               uinteger        not null,
    vessel_config_id        uinteger        not null,
    vessel_type_id          usmallint       not null,
    vessel_name_id          uinteger        not null,
    call_sign_id            uinteger        not null,
    cargo_type_id           usmallint       not null,
    pos_type_id             usmallint       not null,
    start_destination_id    uinteger        not null,
    end_destination_id      uinteger        not null,
    alpha2                  char(2)         not null,
    geom_id                 uinteger        not null,
    meters                  numeric(10, 1)  not null,
    seconds                 uinteger        not null,
    no_points               uinteger        not null,
    tortuosity              float           not null,
    calc_speed_start        numeric(10, 1),
    calc_speed_end          numeric(10, 1),
    calc_speed_max          numeric(10, 1),
    calc_speed_min          numeric(10, 1),
    calc_speed_avg          numeric(10, 1),
    sog_start               numeric(10, 1),
    sog_end                 numeric(10, 1),
    sog_max                 numeric(10, 1),
    sog_min                 numeric(10, 1),
    sog_avg                 numeric(10, 1),
    sog_median              numeric(10, 1),
    draught_start           numeric(10, 1),
    draught_end             numeric(10, 1),
    draught_min             numeric(10, 1),
    draught_max             numeric(10, 1),
    draught_avg             numeric(10, 1),
    draught_median          numeric(10, 1),
    ukc_start               numeric(10, 1),
    ukc_end                 numeric(10, 1),
    ukc_min                 numeric(10, 1),
    ukc_max                 numeric(10, 1),
    ukc_avg                 numeric(10, 1),
    ukc_median              numeric(10, 1),
    prev_obj_id             uinteger,
    prev_obj_type_id        usmallint,
    dist_meter_prev_obj     uinteger,
    dist_sec_prev_obj       uinteger
);


comment on column lakehouse.fact.ais_traj_fact.dist_meter_prev_obj is
    'distance in seconds to the previous object for the vessel, think imputation';
comment on column lakehouse.fact.ais_traj_fact.dist_sec_prev_obj is
    'distance in seconds to the previous object for the vessel, think imputation';

--------------------------------------------------------------------------------------
-- Stops
--------------------------------------------------------------------------------------

-- Where the linestrings stops are stored
create sequence if not exists dim.stop_geom_dim_geom_id_seq;
create table if not exists dim.stop_geom_dim(
    geom_id                 uinteger    primary key default nextval('dim.stop_geom_dim_geom_id_seq'),
    geom                    geometry    not null,
    start_point             geometry    not null,   -- ST_PointN(geom, 1)    
    end_point               geometry    not null,   -- ST_PointN(geom, length(geom))    
    is_simple_geom          boolean     not null,   -- ST_IsSimple(geom)
    is_valid_geom           boolean     not null,   -- ST_IsValid(geom)    
    centroid                geometry    not null,   -- Centroid = ST_Centroid(geom)
    simplified_geom         geometry    not null,   -- ST_ConvexHull(geom) 
    simplified_geom_topo    geometry    not null,   -- ST_SimplifyPreserveTopology(geom)
    check (ST_GeometryType(geom) = 'LINESTRING')
);


create table if not exists lakehouse.fact.ais_stop_fact(
    ais_stop_id             uinteger        not null,
    load_id                 uinteger        not null,
    state_change_id         usmallint       not null,
    start_date_id           uinteger        not null,
    start_time_id           uinteger        not null,
    end_date_id             uinteger        not null,
    end_time_id             uinteger        not null,
    transponder_type_id     usmallint       not null,
    vessel_id               uinteger        not null,
    vessel_config_id        uinteger        not null,
    vessel_type_id          usmallint       not null,
    vessel_name_id          uinteger        not null,
    call_sign_id            uinteger        not null,
    cargo_type_id           usmallint       not null,
    pos_type_id             usmallint       not null,
    start_destination_id    uinteger        not null,
    end_destination_id      uinteger        not null,
    alpha2                  char(2)         not null,
    geom_id                 uinteger        not null,
    meters                  numeric(10, 1)  not null,
    seconds                 uinteger        not null,
    no_points               uinteger        not null,
    tortuosity              float           not null,
    calc_speed_start        numeric(10, 1),
    calc_speed_end          numeric(10, 1),
    calc_speed_max          numeric(10, 1),
    calc_speed_min          numeric(10, 1),
    calc_speed_avg          numeric(10, 1),
    sog_start               numeric(10, 1),
    sog_end                 numeric(10, 1),
    sog_max                 numeric(10, 1),
    sog_min                 numeric(10, 1),
    sog_avg                 numeric(10, 1),
    sog_median              numeric(10, 1),
    draught_start           numeric(10, 1),
    draught_end             numeric(10, 1),
    draught_min             numeric(10, 1),
    draught_max             numeric(10, 1),
    draught_avg             numeric(10, 1),
    draught_median          numeric(10, 1),
    ukc_start               numeric(10, 1),
    ukc_end                 numeric(10, 1),
    ukc_min                 numeric(10, 1),
    ukc_max                 numeric(10, 1),
    ukc_avg                 numeric(10, 1),
    ukc_median              numeric(10, 1),
    prev_obj_id             uinteger,
    prev_obj_type_id        usmallint,
    dist_meter_prev_obj     uinteger,
    dist_sec_prev_obj       uinteger
);
