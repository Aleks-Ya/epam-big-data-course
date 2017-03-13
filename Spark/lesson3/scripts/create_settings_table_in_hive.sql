CREATE TABLE settings (
    ip STRING,
    type INT,
    value DOUBLE,
    period BIGINT
);

SELECT * FROM settings;

INSERT INTO settings(ip, type, value, period) VALUES ("NULL", 1, 0, 0);