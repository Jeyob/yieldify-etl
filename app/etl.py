"""This module contains functions required to load the transformed data into database"""
__all__ = ["transform_and_load"]

import petl
from .utils import db_connect, IPDecoder
from user_agents import parse
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def transform_and_load(filename, db_conn, geo_db_path, seperator='\t'):
    """reads, transforms and loads input data to database

    Since underlying database is sqlite, to avoid transactional overhead with every insert, we bundle
    chunks of insert within a larger transaction then commit. Sqlite can handle bulk inserts better that way
    See (19) on link: https://sqlite.org/faq.html
    """

    conn = db_conn
    curr = conn.cursor()

    # turn off disk sync for performance (https://sqlite.org/pragma.html#pragma_synchronous)
    curr.execute("PRAGMA synchronous = OFF;")
    ip_decoder = IPDecoder(geo_db_path)
    source = petl.io.sources.GzipSource(filename='input_data.gz', remote=False)
    # Extract
    table = petl.fromtext(source=source) \
        .capture('lines', '(.*)\\t(.*)\\t(.*)\\t(.*)\\t(.*)\\t(.*)$',
                 ['date', 'time', 'user_id', 'url', 'ip', 'user_agent'])
    clock1 = petl.clock(table)

    # Transform
    table2 = petl.splitdown(clock1, 'ip', r',\s?') \
        .addfields([('os_family', lambda rec: parse(rec['user_agent']).os.family, 0),
                    ('browser', lambda rec: parse(rec['user_agent']).browser.family, 0),
                    ('country', lambda rec: ip_decoder.country(rec['ip']), 0),
                    ('city', lambda rec: ip_decoder.city(rec['ip']), 0),
                    ('device_type', lambda rec: parse(rec['user_agent']).device.family, 0)])

    clock2 = petl.clock(table2)
    table3 = petl.progress(clock2, 10000)

    curr.execute("BEGIN")
    try:
        petl.todb(table3, curr, 'webanalysis', commit=False)
    except Exception as e:
        conn.rollback()
        raise e
    else:
        conn.commit()
        curr.execute("PRAGMA synchronous = ON;")

    # timing
    logger.info('time for extract: %s seconds' % clock1.time)
    logger.info('time for transform: %s seconds' % (clock2.time - clock1.time))
