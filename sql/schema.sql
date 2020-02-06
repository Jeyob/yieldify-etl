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
    os_family   TEXT
);

CREATE INDEX webanalysis_browser_idx ON webanalysis (browser);
CREATE INDEX webanalysis_os_idx ON webanalysis (os_family);
CREATE INDEX webanalysis_city_idx ON webanalysis (city);
CREATE INDEX webanalysis_country_idx ON webanalysis (country);
