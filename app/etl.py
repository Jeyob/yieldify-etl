"""
This module contains functions required to load the transformed data into database

Available functions:
- transform_and_load: extracts and transforms the data to appropriate format and loads to db
"""
__all__ = ["transform_and_load"]

import petl
from .utils import db_connect
from user_agents import parse
import geoip2.database
from pathlib import Path


def transform_and_load(filename=None, seperator='\t', db_path = ""):
    """reads, transforms and loads input data to database"""

    with db_connect(db_path) as conn:
        ip_reader = geoip2.database.Reader(Path('GeoLite2/GeoLite2-City.mmdb'))
        source = petl.io.sources.GzipSource(filename='input_data.gz', remote=False)
        table = petl.fromtext(source=source) \
            .capture('lines', '(.*)\\t(.*)\\t(.*)\\t(.*)\\t(.*)\\t(.*)$',
                     ['date', 'time', 'user_id', 'url', 'ip', 'user_agent']) \
            .addcolumn('os_family', '') \
            .addcolumn('browser', '') \
            .addcolumn('country', '') \
            .addcolumn('city', '') \
            .cutout('url') \
            .convert({'os_family': lambda v, row: parse(row.user_agent).os.family,
                      'browser': lambda v, row: parse(row.user_agent).browser.family,
                      'city': lambda v, row: ip_reader.city(row.ip).city.name,
                      'country': lambda v, row: ip_reader.city(row.ip).country.name},
                     pass_row=True) \
            .cutout('user_agent')

        table = petl.select(table,
                           lambda rec: rec.city is not None and \
                                       rec.country is not None and \
                                       rec.os_family is not None and \
                                       rec.browser is not None)

        petl.todb(table, conn, 'webanalysis')