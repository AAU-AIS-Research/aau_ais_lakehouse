CREATE OR REPLACE MACRO ais.stot(date_key, time_key) AS
    try_strptime(
        lpad(date_key::VARCHAR, 8, '0')
        || lpad(time_key::VARCHAR, 6, '0'),
        '%Y%m%d%H%M%S'
    );