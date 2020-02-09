DROP TABLE IF EXISTS webanalysis;
CREATE TABLE webanalysis (
    user_id     TEXT,
    date        TEXT,
    time        TEXT,
    url         TEXT,
    ip          TEXT,
    country     TEXT,
    city        TEXT,
    user_agent  TEXT,
    browser     TEXT,
    os_family   TEXT,
    device_type TEXT
);
