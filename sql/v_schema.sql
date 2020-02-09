CREATE OR REPLACE VIEW v_browser_breakdown AS
SELECT browser,
       ROUND(CAST(share as float)/cast(total as float), 10) as percentage
FROM (
       SELECT *,
              count(browser) OVER (PARTITION BY browser) as share,
              count(browser) OVER () AS total
       FROM webanalysis
     )
GROUP BY browser
ORDER BY percentage DESC;


CREATE OR REPLACE VIEW v_os_breakdown AS
SELECT os_family,
       round(cast(share AS float)/cast(total AS float), 10) AS percentage
FROM (
       SELECT *,
              count(os_family) OVER (PARTITION BY os_family) AS share,
              count(os_family) OVER () AS total
       FROM webanalysis
     )
GROUP BY os_family
ORDER BY percentage DESC LIMIT 5;


CREATE OR REPLACE VIEW v_device_type_breakdown AS
SELECT device_type,
       round(cast(share AS float)/cast(total AS float), 10) AS percentage
FROM (
       SELECT *,
              count(device_type) OVER (PARTITION BY device_type) AS share,
              count(device_type) OVER () AS total
       FROM webanalysis
     )
GROUP BY device_type
ORDER BY percentage DESC LIMIT 5;

