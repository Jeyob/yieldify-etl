__all__ = ["get_browser_stat", "get_device_stat", "get_os_stat"]

from . import api
from flask import request, abort, jsonify
from .. import TopOSByUniqueUsers, TopBrowsersByUniqueUsers, TopCitiesByEvents, TopCountriesByEvents, \
    DeviceTypeBreakdown, OSDetectedBreakdown, BrowserDetectedBreakdown
import os
import pyhocon
from .. import utils
import logging
from datetime import datetime
import re

logger = logging.getLogger(__name__)
regex = re.compile(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}')


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def is_timestamp(timestamp: str):
    return True if regex.match(timestamp) else False


def get_stats(query_params, func):

    config = pyhocon.ConfigFactory.parse_file(os.environ["YIELDIFY_CONFIG"])
    db_conn = utils.db_connect(config.connections.databases.sqlite.path)
    db_conn.row_factory = dict_factory

    try:

        start_date = query_params.get('start_date')
        end_date = query_params.get('end_date')

        if not (is_timestamp(start_date) and is_timestamp(end_date)):
            raise ValueError("Not timestamp formatted strings")

        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

        if (start_date and not end_date) or (not start_date and end_date):
            abort(400)

        if start_date and end_date:
            result = func(start_date=start_date, end_date=end_date).execute(db_conn=db_conn)
        else:
            result = func().execute(db_conn=db_conn)

    except ValueError as e:
        logger.error(e)
        abort(400)
    else:
        return jsonify(result)
    finally:
        db_conn.close()


@api.route("/stat/browser", methods=['GET'])
def get_browser_stat():
    result = get_stats(request.args, BrowserDetectedBreakdown)
    return result


@api.route("/stat/os", methods=['GET'])
def get_os_stat():
    result = get_stats(request.args, OSDetectedBreakdown)
    return result


@api.route("/stat/device", methods=['GET'])
def get_device_stat():
    result = get_stats(request.args, DeviceTypeBreakdown)
    return result
