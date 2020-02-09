__all__ = ["create_app"]
from . utils import *
from . etl import *
from . analysis import *
from . api_0_1 import *
__all__ += utils.__all__
__all__ += etl.__all__
__all__ += analysis.__all__
__all__ += api_0_1.__all__


from flask import Flask, jsonify
from .api_0_1 import api as api_0_1_blueprint


def create_app() -> Flask:
    flask_app = Flask(__name__)
    flask_app.register_blueprint(api_0_1_blueprint)
    return flask_app
