from .client import Client

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

__version__ = '0.0.1'

__all__ = [
    '__version__',
    'Client'
]
