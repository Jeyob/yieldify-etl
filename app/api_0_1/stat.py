__all__ = ["get_browser_stat", "get_device_stat", "get_os_stat", "is_timestamp"]

from . import api
from flask import request, abort, jsonify
from .. import TopOSByUniqueUsers, TopBrowsersByUniqueUsers, TopCitiesByEvents, TopCountriesByEvents, \
    DeviceTypeBreakdown, OSDetectedBreakdown, BrowserDetectedBreakdown
from .error import BadRequest
import os
import pyhocon
from .. import utils
import logging
from datetime import datetime
import re

logger = logging.getLogger(__name__)
regex = re.compile(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}')


# stolen with pride from:
# https://programminghistorian.org/en/lessons/creating-apis-with-python-and-flask#api-design-principles
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def is_timestamp(timestamp: str):
    """
    Verifies whether string is of timestamp format (YYYY-MM-DD HH:MM:SS)
    :param timestamp: timestamp string
    :return: True: if match with regex, False otherwise
    """
    return True if regex.match(timestamp) else False


def get_stats(query_params, func):
    """
    Invoked by each endpoint route, fetches the response by calling the specific calculation method
    :param query_params: url query parameters, if any.
    :param func: the analysis method to run
    :return: the response set (json format)

    :raises BadRequest: if provided input deviates from expected.
    """
    config = pyhocon.ConfigFactory.parse_file(os.environ["YIELDIFY_CONFIG"])
    db_conn = utils.db_connect(config.connections.databases.sqlite.path)
    db_conn.row_factory = dict_factory

    try:

        start_date = query_params.get('start_date')
        end_date = query_params.get('end_date')

        if (start_date is None and end_date is not None) or (start_date is not None and end_date is None):
            raise BadRequest(error_message="both query params must be defined for interval search")

        if start_date and end_date:
            if is_timestamp(start_date) and is_timestamp(end_date):
                raise BadRequest(error_message="query parameters not valid timestamp format")

            start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
            end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
            result = func(start_date=start_date, end_date=end_date).execute(db_conn=db_conn)
        else:
            result = func().execute(db_conn=db_conn)

    except ValueError as e:
        logger.error(e)
        raise BadRequest(error_message="ValueError, verify that your request is valid")
    else:
        return jsonify(result)
    finally:
        db_conn.close()


@api.route("/stats/browser", methods=['GET'])
def get_browser_stat():
    try:
        result = get_stats(request.args, BrowserDetectedBreakdown)
    except BadRequest as e:
        return e.to_json()
    else:
        return result


@api.route("/stats/os", methods=['GET'])
def get_os_stat():
    result = get_stats(request.args, OSDetectedBreakdown)
    return result


@api.route("/stats/device", methods=['GET'])
def get_device_stat():
    result = get_stats(request.args, DeviceTypeBreakdown)
    return result
