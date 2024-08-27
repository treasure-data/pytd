import importlib.metadata
import logging

from .client import Client

__version__ = importlib.metadata.version("pytd")
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

__all__ = ["__version__", "Client"]
