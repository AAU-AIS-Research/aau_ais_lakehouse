install spatial;
install ducklake;

load spatial;
load ducklake;

set global memory_limit = '20GiB'; -- Adjust based on your system

attach 'ducklake:/opt/gizmosql/ducklake.catalog' as lakehouse (DATA_PATH '/opt/gizmosql/ducklake');