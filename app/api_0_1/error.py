__all__ = ["bad_request", "not_found"]
from flask import jsonify, make_response
from . import api


class ErrorResponse:

    def __init__(self, http_status, error_code, error_message):
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


class NotFound(ErrorResponse):
    """ error message for HTTP 404 response code"""

    def __init__(self, http_status=404, error_code="Not Found", error_message=""):
        super().__init__(404, error_code, error_message)


@api.errorhandler(400)
def bad_request(error):
    return make_response(
        BadRequest(error_code="Bad request", error_message="Review your request")
    )


@api.errorhandler(404)
def not_found(error):
    return make_response(
        BadRequest(error_code="Not Found", error_message="Resource could not be found, review request")
    )
