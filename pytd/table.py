import re
from typing import TYPE_CHECKING, Any, Literal

import pandas as pd
from tdclient.errors import NotFoundError

from .writer import Writer

if TYPE_CHECKING:
    from .client import Client


class Table:
    """A table controller module.

    Parameters
    ----------
    client : :class:`pytd.Client`
        Treasure Data client.

    database : str
        Database name.

    table : str
        Table name.

    Attributes
    ----------
    client : :class:`pytd.Client`
        Treasure Data client.

    database : str
        Database name.

    table : str
        Table name.
    """

    def __init__(self, client: "Client", database: str, table: str) -> None:
        try:
            client.api_client.database(database)
        except NotFoundError as e:
            raise ValueError(
                "failed to create pytd.table.Table instance for "
                f"`{database}.{table}`: {e}"
            ) from e

        self.database: str = database
        self.table: str = table
        self.client: Client = client

    @property
    def exists(self) -> bool:
        """Check if a configured table exists.

        Returns
        -------
        bool
        """
        try:
            self.client.api_client.table(self.database, self.table)
        except NotFoundError:
            return False
        return True

    def create(
        self,
        column_names: list[str] | None = None,
        column_types: list[str] | None = None,
    ) -> None:
        """Create a table named as configured.

        When ``column_names`` and ``column_types`` are given, table is created
        by a Presto query with the specified schema.

        Parameters
        ----------
        column_names : list of str, optional
            Column names.

        column_types : list of str, optional
            Column types corresponding to the names. Note that Treasure Data
            supports limited amount of types as documented in:
            https://docs.treasuredata.com/display/public/PD/Schema+Management
        """
        if column_names is None:
            column_names = []
        if column_types is None:
            column_types = []

        if len(column_names) > 0:
            schema = ", ".join(
                map(
                    lambda t: f"{t[0]} {t[1]}",
                    zip(column_names, column_types, strict=False),
                )
            )
            q_create = f"CREATE TABLE {self.database}.{self.table} ({schema})"
            self.client.query(q_create, engine="presto")
        else:
            self.client.api_client.create_log_table(self.database, self.table)

    def delete(self) -> None:
        """Delete a table from Treasure Data."""
        self.client.api_client.delete_table(self.database, self.table)

    def import_dataframe(
        self,
        dataframe: pd.DataFrame,
        writer: Literal["bulk_import", "insert_into", "spark"] | Writer,
        if_exists: Literal["error", "overwrite", "append", "ignore"] = "error",
        **kwargs: Any,
    ) -> None:
        """Import a given DataFrame to a Treasure Data table.

        Parameters
        ----------
        dataframe : :class:`pandas.DataFrame`
            Data loaded to a target table.

        writer : string, {'bulk_import', 'insert_into', 'spark'}, or \
                    :class:`pytd.writer.Writer`
            A Writer to choose writing method to Treasure Data. If string
            value, a temporal Writer instance will be created.

        if_exists : str, {'error', 'overwrite', 'append', 'ignore'}, default: 'error'
            What happens when a target table already exists.

            - error: raise an exception.
            - overwrite: drop it, recreate it, and insert data.
            - append: insert data. Create if does not exist.
            - ignore: do nothing.
        """
        # normalize column names so it contains only alphanumeric and `_`
        dataframe = dataframe.rename(
            lambda c: re.sub(r"[^a-zA-Z0-9]", " ", str(c)).lower().replace(" ", "_"),
            axis="columns",
        )

        writer_from_string = isinstance(writer, str)

        if writer_from_string:
            writer = Writer.from_string(writer, **kwargs)

        writer.write_dataframe(dataframe, self, if_exists, **kwargs)

        if writer_from_string:
            writer.close()
