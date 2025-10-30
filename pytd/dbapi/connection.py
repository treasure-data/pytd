from types import TracebackType
from typing import TYPE_CHECKING

from .error import NotSupportedError

if TYPE_CHECKING:
    from ..client import Client
    from ..query_engine import Cursor


class Connection:
    """The DBAPI interface to Treasure Data.

    https://www.python.org/dev/peps/pep-0249/

    The interface internally bundles pytd.Client. Implementation is technically
    based on the DBAPI interface to Treasure Data's Presto query engine, which
    relies on trino-python-client:
    https://github.com/trinodb/trino-python-client.

    Parameters
    ----------
    client : pytd.Client, optional
        A client used to connect to Treasure Data.
    """

    def __init__(self, client: "Client") -> None:
        self.client: Client = client

    def close(self) -> None:
        self.client.close()

    def commit(self) -> None:
        raise NotSupportedError

    def rollback(self) -> None:
        raise NotSupportedError

    def cursor(self) -> "Cursor":
        return self.client.default_engine.cursor()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
