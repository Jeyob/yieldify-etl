DROP INDEX IF EXISTS webanalysis_browser_idx;
CREATE INDEX webanalysis_browser_idx ON webanalysis (browser);
DROP INDEX IF EXISTS webanalysis_os_idx;
CREATE INDEX webanalysis_os_idx ON webanalysis (os_family);
DROP INDEX IF EXISTS webanalysis_city_idx;
CREATE INDEX webanalysis_city_idx ON webanalysis (city);
DROP INDEX IF EXISTS webanalysis_country_idx;
CREATE INDEX webanalysis_country_idx ON webanalysis (country);