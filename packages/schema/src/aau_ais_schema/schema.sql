--------------------------------------------------------------------------------------
--#region Schema
--------------------------------------------------------------------------------------
create schema if not exists dim;
create schema if not exists lakehouse.fact;

set variable max_uint = 4_294_967_295;
--#endregion
--------------------------------------------------------------------------------------
--#region Macros
--------------------------------------------------------------------------------------
create or replace macro minutes_since_midnight(ts) as
    hour(ts::timestamp) * 60 + minute(ts::timestamp);

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

create or replace macro quadkey_uint16_encode(qkey) as (
    select quadkey_bit_encode(qkey)::usmallint as qkey
);

create or replace macro quadkey_uint32_encode(qkey) as (
    select quadkey_bit_encode(qkey)::uinteger as qkey
);

create or replace macro quadkey_uint64_encode(qkey) as (
    select quadkey_bit_encode(qkey)::ubigint as qkey
);

create or replace macro quadkey_int16_encode(qkey) as (
    select quadkey_bit_encode(qkey)::int16 as qkey
);

create or replace macro quadkey_int32_encode(qkey) as (
    select quadkey_bit_encode(qkey)::int32 as qkey
);

create or replace macro quadkey_int64_encode(qkey) as (
    select quadkey_bit_encode(qkey)::int64 as qkey
);

create or replace macro quadkey_to_zxy(qkey) as table (
    with recursive
    digits(qkey) as (
        select
            qkey,
            length(qkey)::int           as z,
            pos + 1                     as pos,
            substring(qkey, pos + 1, 1) as digit
        from range(length(qkey))        as t(pos)
    ),
    recur(pos, digit, z, x, y) as (
        -- base case
        select 0, null, (select z from digits limit 1), 0::int, 0::int
        union all
        -- recursive step
        select
            d.pos,
            d.digit,
            r.z,
            r.x * 2 + case d.digit when '1' then 1 when '3' then 1 else 0 end,
            r.y * 2 + case d.digit when '2' then 1 when '3' then 1 else 0 end
        from recur r
        join digits d on d.pos = r.pos + 1
    )
    select
        z,
        x,
        y
    from recur
    where pos = z
);

create or replace macro int_to_quadkey(val, z) as (
    with input as (
        select right(val::bit::varchar, z * 2) as bitstring
    )
    select string_agg(digit, '' order by pos) as quadkey
    from (
    select
        case substr(bitstring, pos, 2)
        when '00' then '0' when '01' then '1' when '10' then '2' when '11' then '3'
        end as digit,
        pos
    from input, generate_series(1, length(bitstring), 2) as gs(pos)
    order by pos
    ) t
);

create or replace macro coord_to_quadrant(lon, lat) as (
    select case
        when lon >= 0 and lat >= 0 then 1
        when lon < 0 and lat >= 0 then 2
        when lon < 0 and lat < 0 then 3
        when lon >=0 and lat < 0 then 4
    end     
);

create or replace macro coord_to_grid_id(x, y, x_min, y_min, x_max, y_max, cell_width, cell_height) AS (
    with variables as (
        select floor((x_max - x_min) / cell_width)  as col_cnt,
               floor((y_max - y_min) / cell_height) as row_cnt,
               floor((x - x_min) / cell_width)      as i,
               floor((y - y_min) / cell_height)     as j
    )
    select
        case
            when 0 > i or i >= col_cnt or 0 > j or j >= row_cnt or isinf(x) or isinf(y) then -1
            else (col_cnt * j + i)::bigint
        end as grid_id
    from variables
);

create or replace macro wgs84_coord_to_grid_id(lon, lat, cell_width, cell_height) as (
    with wgs84_convert as (
        select if(lon = 180, -180, lon) + 180   as lon_360,
               lat + 90                         as lat_180,
    ), variable as (
        select lon_360,
               lat_180,
               coord_to_quadrant(lon_360 - 180, lat_180 - 90)   as quadrant,
               if(lon_360 < 180, 0, 180)                        as x_min,
               if(lat_180 < 90, 0, 90)                          as y_min,
               if(lon_360 < 180, 180, 360)                      as x_max,
               if(lat_180 < 90, 90, 180)                        as y_max
        from wgs84_convert
    ), result as (
        select
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
            ) as grid_id
        from variable
    )
    select if(grid_id = -1, -1, grid_id + quadrant * 100000)
    from result
);

create or replace macro grid_id_to_coord(id, x_min, y_min, x_max, y_max, cell_width, cell_height) as (
    with variable as (
        select
            floor((x_max - x_min) / cell_width)   as col_cnt,
            floor((y_max - y_min) / cell_height)  as row_cnt
    )
    select
        case
            when id between 0 and (col_cnt * row_cnt) - 1 then
                {'x': id % col_cnt * cell_width + x_min, 'y': floor(id / col_cnt) * cell_height + y_min}
            else {'x': null, 'y': null}
        end
    from variable
);

create or replace macro grid_id_to_wgs84_coord(grid_id, cell_width, cell_height) as (
    with variable as (
        select
            floor(grid_id / 100000) as quadrant,
            grid_id % 100000        as v_grid_id
    ), to_coord as (
        select 
            case
                when quadrant = 1 then
                    grid_id_to_coord(v_grid_id, 180, 90, 360, 180, cell_width, cell_height)
                when quadrant = 2 then
                    grid_id_to_coord(v_grid_id, 0, 90, 180, 180, cell_width, cell_height)
                when quadrant = 3 then
                    grid_id_to_coord(v_grid_id, 0, 0, 180, 90, cell_width, cell_height)
                when quadrant = 4 then
                    grid_id_to_coord(v_grid_id, 180, 0, 360, 90, cell_width, cell_height)
            end as coord
        from variable
    )
    select {'x': coord.x - 180, 'y': coord.y - 90}
    from to_coord
);

create or replace macro grid_id_to_wgs84_envelope(grid_id, cell_width, cell_height) as (
    with variable as (
        select grid_id_to_wgs84_coord(grid_id, cell_width, cell_height) as coord
    )
    select ST_MakeEnvelope(coord.x, coord.y, coord.x + cell_width, coord.y + cell_height)
    from variable
);
--#endregion
--------------------------------------------------------------------------------------
--#region Dimensions
--------------------------------------------------------------------------------------
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
insert or ignore into dim.country_dim values ('??', '???', 'unknown', 0, null, null, null, null, null, null);

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

-- Depth cell dimension
create sequence if not exists dim.depth_grid_dim_depth_cell_id_seq;
create table if not exists dim.depth_grid_dim(
    depth_cell_id   ubigint     primary key default nextval('dim.depth_grid_dim_depth_cell_id_seq'),
    x               uinteger    not null,
    y               uinteger    not null,
    depth           float       not null check (depth >= 0),
    partly_land     boolean     default null,
    derived_value   boolean     default null,
    geom            geometry    not null,
    unique (x, y)
);
comment on column dim.depth_grid_dim.depth is 'Depth in meters';
comment on column dim.depth_grid_dim.geom is 'SRID is 3034';
insert or ignore into dim.depth_grid_dim(x, y, depth, geom) values 
    (getvariable('max_uint'), getvariable('max_uint'), 0, 'POLYGON EMPTY');

-- Half-degree grid dimension
create sequence if not exists dim.half_degree_grid_dim_half_degree_cell_id_seq;
create table if not exists dim.half_degree_grid_dim(
    half_degree_cell_id uinteger    primary key default nextval('dim.half_degree_grid_dim_half_degree_cell_id_seq'),
    gst_cell_id         varchar(5)
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
insert or ignore into dim.vessel_name_dim(vessel_name) values ('UNKNOWN');

-- Vessel positioning type dimension
create sequence if not exists dim.pos_type_dim_pos_type_id_seq;
create table if not exists dim.pos_type_dim(
    pos_type_id usmallint       primary key default nextval('dim.pos_type_dim_pos_type_id_seq'),
    pos_type    varchar(250)    not null unique
);
insert or ignore into dim.pos_type_dim(pos_type) values ('unknown');

-- Vessel cargo type dimension
create sequence if not exists dim.cargo_type_dim_cargo_type_id_seq;
create table if not exists dim.cargo_type_dim(
    cargo_type_id   usmallint   primary key default nextval('dim.cargo_type_dim_cargo_type_id_seq'),
    cargo_type      varchar(50) not null unique
);
insert or ignore into dim.cargo_type_dim(cargo_type) values ('unknown');

-- Vessel call-sign dimension
create sequence if not exists dim.call_sign_dim_call_sign_id_seq;
create table if not exists dim.call_sign_dim(
    call_sign_id    uinteger        primary key default nextval('dim.call_sign_dim_call_sign_id_seq'),
    call_sign       varchar(250)    not null unique
);
insert or ignore into dim.call_sign_dim(call_sign) values ('UNKNOWN');

-- Vessel destination dimension
create sequence if not exists dim.destination_dim_destination_id_seq;
create table if not exists dim.destination_dim(
    destination_id  uinteger    primary key default nextval('dim.destination_dim_destination_id_seq'),
    org_msg         text        unique          not null
);
insert or ignore into dim.destination_dim(org_msg) values ('unknown');

-- Navigation status dimension
create sequence if not exists dim.nav_status_dim_nav_status_id_seq;
create table if not exists dim.nav_status_dim(
    nav_status_id   usmallint       primary key default nextval('dim.nav_status_dim_nav_status_id_seq'),
    nav_status      varchar(250)    not null unique
);
insert or ignore into dim.nav_status_dim(nav_status) values ('unknown');

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

--#endregion
--------------------------------------------------------------------------------------
--#region Facts
--------------------------------------------------------------------------------------

-- AIS point fact
create table if not exists lakehouse.fact.ais_point_fact(
    ais_point_id        ubigint     not null,
    prev_ais_point_id   ubigint,
    temporal_part_key   uinteger    not null,
    spatial_part_key    uinteger    not null,
    qkey                ubigint     not null,
    alpha2              char(2)     not null,
    load_id             uinteger    not null,
    date_id             uinteger    not null,
    time_id             uinteger    not null,
    call_sign_id        uinteger    not null,
    cargo_type_id       usmallint   not null,
    depth_cell_id       ubigint     not null,
    destination_id      uinteger    not null,
    half_degree_cell_id uinteger    not null,
    nav_status_id       usmallint   not null,
    pos_type_id         usmallint   not null,
    transponder_type_id usmallint   not null,
    vessel_id           uinteger    not null,
    vessel_config_id    uinteger    not null,
    vessel_type_id      usmallint   not null,
    vessel_name_id      uinteger    not null,
    ts                  timestamp   not null,
    lon                 float       not null,
    lat                 float       not null,
    rot                 numeric(10, 1),
    sog                 numeric(10, 1),
    cog                 numeric(10, 1),
    heading             numeric(10, 1),
    draught             numeric(10, 1),
    depth               float,
    eta                 timestamp,
    ukc                 float,
    delta_pos           float,
    delta_sec           float,
    delta_sog           numeric(10,1),
    delta_rot           numeric(10,1),
    delta_cog           numeric(10,1),
    delta_heading       numeric(10,1),
    delta_cog_heading   numeric(10,1),
    delta_draught       numeric(10,1),
    delta_destination   boolean
);

alter table lakehouse.fact.ais_point_fact set partitioned by (spatial_part_key, temporal_part_key);

-- AIS trajectory fact
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

-- AIS stop fact
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
--#endregion
--------------------------------------------------------------------------------------
