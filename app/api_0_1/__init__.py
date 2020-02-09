__all__ = []

from flask import Blueprint
api = Blueprint('api', __name__)

# importing after api declaration due to dependency
from . error import *
__all__ += error.__all__
from . stat import *
__all__ += stat.__all__
