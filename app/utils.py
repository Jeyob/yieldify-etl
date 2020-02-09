"""
Library of auxiliary functions

Available functions:
- read_gip: opens a compressed gzip file and returns filedescriptor
-
"""
__all__ = ['read_gzip', 'IPDecoder', 'db_connect']


# standard libs
import gzip
import logging
from pathlib import Path
import io
import sqlite3
# 3rd party libs
from geoip2 import database, errors

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


class IPDecoder:
    def __init__(self, db_path):
        self.db_path = db_path
        self.reader = database.Reader(Path(self.db_path))

    def city(self, ipaddress):
        try:
            return self.reader.city(ipaddress).city.name
        except ValueError as e:
            return 'Invalid IP'
        except errors.AddressNotFoundError as e:
            return 'Not found'
        except Exception:
            pass

    def country(self, ipaddress):
        try:
            return self.reader.city(ipaddress).country.name
        except ValueError as e:
            return 'Invalid IP'
        except errors.AddressNotFoundError as e:
            return 'Not found'
        except Exception:
            pass

    def close(self):
        self.reader.close()


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
