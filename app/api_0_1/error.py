__all__ = ["bad_request"]
from flask import jsonify, make_response
from . import api
from werkzeug.exceptions import HTTPException


class ErrorResponse(Exception):

    def __init__(self, http_status, error_code, error_message):
        super().__init__(self)
        self.http_status = http_status
        self.error_code = error_code
        self.error_message = error_message

    def to_json(self):
        response = jsonify(
            {
                'error_status': self.http_status,
                'error_code': self.error_code,
                'error_message': self.error_message
            }
        )

        return response


class BadRequest(ErrorResponse):
    """ error message for HTTP 400 response code"""

    def __init__(self, error_code="Bad Request", error_message=""):
        super().__init__(400, error_code, error_message)


@api.errorhandler(BadRequest)
def bad_request(error):
    return error.to_json()




