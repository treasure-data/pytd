import logging

import pkg_resources

from .client import Client

__version__ = pkg_resources.get_distribution("pytd").version
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

__all__ = ["__version__", "Client"]
