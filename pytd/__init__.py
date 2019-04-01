from .version import __version__
from .client import Client

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

__all__ = [
    '__version__',
    'Client'
]
