"""
Library of auxiliary functions

Available functions:
- read_gip: opens a compressed gzip file and returns filedescriptor
-
"""
__all__ = ['read_gzip', 'ip_geocoding', 'db_connect']


# standard libs
import gzip
import logging
from pathlib import Path
import io
import sqlite3
# 3rd party libs
import geoip2.database

logger = logging.getLogger(__name__)


def read_gzip(filename):
    """
    Opens a compressed gzip file and returns a filestream descriptor
    :param filename: path to compressed gzip file
    :return: file descriptor to file
    """
    path = Path(filename)
    bfr = None
    if not path.exists():
        raise FileNotFoundError()

    try:
        gzfile= gzip.open(filename, mode='r')
        bfr = io.BufferedReader(gzfile)
    except Exception as e:
        logger.error(f"{__name__}: failed to open gzip")

    return bfr


def ip_geocoding(ipaddress) -> (str, str):
    """
    Derives country and city from IP address
    :param ipaddress:
    :return:
    """
    reader = None
    try:
        reader = geoip2.database.Reader(Path('GeoLite2/GeoLite2-City.mmdb'))
        response = reader.city(ip_address=ipaddress)
        return (response.country.name, response.city.name)
    except Exception as e:
        print(e)
    finally:
        reader.close()


def db_connect(path: str) -> sqlite3.Connection:
    """Create and return a db connection

    Function will try to connect to db specified in path, or create a new db file if file do not exist

    Args:
        path: string specifying the location of the database
    Returns:
        A sqlite connection object
    """
    try:
        conn = sqlite3.connect(Path(path))
        logger.info(f"{__name__}: db connection established")
    except Exception as e:
        logger.error(f"{__name__}: could not connect to database {path}", exc_info=True)
    else:
        return conn
