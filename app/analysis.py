__all__ = ["top_countries_by_events", "top_cities_by_events", "top_browsers_by_unique_users", "top_os_by_unique_users"]

import sqlite3
import logging

logger = logging.getLogger(__name__)


def top_countries_by_events(db_conn: sqlite3.Connection, n=5):
    query = "select country, count(*) as events " \
            "from webanalysis " \
            "group by country " \
            "order by events desc " \
            "limit ?;"
    curr = db_conn.cursor()
    try:
        curr.execute(query, (n,))
        rows = curr.fetchall()
        return rows
    except sqlite3.Error as err:
        logger.error(f"Sqlite error: Calling {__name__} failed", exc_info=True)
    except Exception as err:
        logger.error(f"Unexpected error {__name__}", exc_info=True)
    finally:
        curr.close()


def top_cities_by_events(db_conn: sqlite3.Connection, n=5):
    query = "select city, count(city) as events " \
            "from webanalysis " \
            "group by country, city " \
            "order by events desc " \
            "limit ?;"
    curr = db_conn.cursor()
    try:
        curr.execute(query, (n,))
        rows = curr.fetchall()
        return rows
    except sqlite3.Error as err:
        logger.error(f"Sqlite error: Calling {__name__} failed", exc_info=True)
    except Exception as err:
        logger.error(f"Unexpected error {__name__}", exc_info=True)
    finally:
        curr.close()


def top_browsers_by_unique_users(db_conn: sqlite3.Connection, n=5):
    query = "select browser, count(distinct user_id) as unique_users " \
            "from webanalysis " \
            "group by browser " \
            "order by unique_users desc " \
            "limit ?;"

    curr = db_conn.cursor()
    try:
        curr.execute(query, (n,))
        rows = curr.fetchall()
        return rows
    except sqlite3.Error as err:
        logger.error(f"Sqlite error: Calling {__name__} failed", exc_info=True)
    except Exception as err:
        logger.error(f"Unexpected error {__name__}", exc_info=True)
    finally:
        curr.close()


def top_os_by_unique_users(db_conn: sqlite3.Connection, n=5):
    query = "select os_family, count(distinct user_id) as unique_users " \
            "from webanalysis " \
            "group by os_family " \
            "order by unique_users desc " \
            "limit ?;"
    curr = db_conn.cursor()
    try:
        curr.execute(query, (n,))
        rows = curr.fetchall()
        return rows
    except sqlite3.Error as err:
        logger.error(f"Sqlite error: Calling {__name__} failed", exc_info=True)
    except Exception as err:
        logger.error(f"Unexpected error {__name__}", exc_info=True)
    finally:
        curr.close()