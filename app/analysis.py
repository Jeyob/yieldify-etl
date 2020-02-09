__all__ = ["TopCountriesByEvents",
           "TopCitiesByEvents",
           "TopBrowsersByUniqueUsers",
           "TopOSByUniqueUsers",
           "DeviceTypeBreakdown",
           "OSDetectedBreakdown",
           "BrowserDetectedBreakdown"]

import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class Query:
    query = None
    start_date = None
    end_date = None

    def __init__(self, query, start_date=None, end_date=None):
        self.query = query
        self.start_date = start_date
        self.end_date = end_date

    def execute(self, db_conn):
        curr = db_conn.cursor()
        try:
            if self.start_date and self.end_date:
                curr.execute(
                    self.query,
                    (
                        self.start_date.strftime("%Y-%m-%d %H:%M:%S"),
                        self.end_date.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                )
            else:
                curr.execute(self.query)
            rows = curr.fetchall()
            return rows
        except sqlite3.Error as err:
            logger.error(f"Sqlite error: Calling {__name__} failed", exc_info=True)
        except Exception as err:
            logger.error(f"Unexpected error {__name__}", exc_info=True)
        finally:
            curr.close()


class TopCountriesByEvents(Query):
    def __init__(self, start_date=None, end_date=None):
        super().__init__(
            "".join(["select country ",
                     "from webanalysis ",
                     "where date || ' ' || time >= datetime(?) AND date || ' ' || time <= datetime(?)"
                     if start_date and end_date else "",
                     "group by country ",
                     "order by count(*) desc limit 5"]),
            start_date,
            end_date
        )


class TopCitiesByEvents(Query):
    def __init__(self, start_date=None, end_date=None):
        super().__init__(
            "".join(["select city ",
                     "from webanalysis ",
                     "where date || ' ' || time >= datetime(?) AND date || ' ' || time <= datetime(?)"
                     if start_date and end_date else "",
                     "group by country, city ",
                     "order by count(city) desc limit 5"]),
            start_date,
            end_date
        )


class TopBrowsersByUniqueUsers(Query):
    def __init__(self, start_date=None, end_date=None):
        super().__init__(
            "".join(["select browser ",
                     "from webanalysis ",
                     "where date || ' ' || time >= datetime(?) AND date || ' ' || time <= datetime(?)"
                     if start_date and end_date else "",
                     "group by browser ",
                     "order by count(distinct user_id) desc limit 5"]),
            start_date,
            end_date
        )


class TopOSByUniqueUsers(Query):
    def __init__(self, start_date=None, end_date=None):
        super().__init__(
            "".join(["select os_family ",
                     "from webanalysis ",
                     "where date || ' ' || time >= datetime(?) AND date || ' ' || time <= datetime(?)"
                     if start_date and end_date else "",
                     "group by os_family ",
                     "order by count(distinct user_id) desc limit 5"]),
            start_date,
            end_date
        )


class OSDetectedBreakdown(Query):
    def __init__(self, start_date=None, end_date=None):
        super().__init__(
            "".join(["SELECT os_family,",
                     "round(CAST(share AS float)/cast(total AS float), 10) AS percentage ",
                     "FROM (SELECT *, COUNT(os_family) OVER (PARTITION BY os_family) AS share,"
                     "COUNT(os_family) OVER () AS total ",
                     "FROM webanalysis ",
                     "WHERE date || ' ' || time >= datetime(?) AND date || ' ' || time <= datetime(?)"
                     if start_date and end_date else "",
                     ") GROUP BY os_family ",
                     "ORDER BY percentage DESC LIMIT 5;"]),
            start_date,
            end_date
        )


class BrowserDetectedBreakdown(Query):
    def __init__(self, start_date=None, end_date=None):
        super().__init__(
            "".join(["SELECT browser,",
                     "round(CAST(share AS float)/cast(total AS float), 10) AS percentage ",
                     "FROM (SELECT *, COUNT(browser) OVER (PARTITION BY browser) AS share,"
                     "COUNT(browser) OVER () AS total ",
                     "FROM webanalysis ",
                     "WHERE date || ' ' || time >= datetime(?) AND date || ' ' || time <= datetime(?)"
                     if start_date and end_date else "",
                     ") GROUP BY browser ",
                     "ORDER BY percentage DESC LIMIT 5;"]),
            start_date,
            end_date
        )


class DeviceTypeBreakdown(Query):
    def __init__(self, start_date=None, end_date=None):
        super().__init__(
            "".join(["SELECT device_type,",
                     "round(CAST(share AS float)/cast(total AS float), 10) AS percentage ",
                     "FROM (SELECT *, COUNT(device_type) OVER (PARTITION BY device_type) AS share,"
                     "COUNT(device_type) OVER () AS total ",
                     "FROM webanalysis ",
                     "WHERE date || ' ' || time >= datetime(?) AND date || ' ' || time <= datetime(?)"
                     if start_date and end_date else "",
                     ") GROUP BY device_type ",
                     "ORDER BY percentage DESC LIMIT 5;"]),
            start_date,
            end_date
        )
