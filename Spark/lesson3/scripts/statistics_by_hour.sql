CREATE TABLE statistics_by_hour (
    time_stamp STRING,
    ip STRING,
    traffic_consumed BIGINT,
    average_speed DOUBLE
);

SELECT * FROM statistics_by_hour;

TRUNCATE TABLE statistics_by_hour;
