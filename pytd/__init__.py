import logging

from .client import Client
from .version import __version__

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

__all__ = ["__version__", "Client"]
