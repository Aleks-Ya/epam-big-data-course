CREATE TABLE settings (
    ip STRING,
    type INT,
    value DOUBLE,
    period BIGINT
);

SELECT * FROM settings;

INSERT INTO settings(ip, type, value, period) VALUES ("NULL", 2, 1, 10); --default
INSERT INTO settings(ip, type, value, period) VALUES ("NULL", 1, 2, 20); --default

INSERT INTO settings(ip, type, value, period) VALUES ("173.194.73.101", 1, 3, 20); --google.com
INSERT INTO settings(ip, type, value, period) VALUES ("173.194.73.101", 2, 2, 10); --google.com

INSERT INTO settings(ip, type, value, period) VALUES ("10.16.0.4", 1, 4, 20); --epam.com
INSERT INTO settings(ip, type, value, period) VALUES ("10.16.0.4", 2, 3, 10); --epam.com