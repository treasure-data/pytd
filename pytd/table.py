import logging
import re

import tdclient

from .writer import Writer

logger = logging.getLogger(__name__)

TD_SPARK_BASE_URL = "https://s3.amazonaws.com/td-spark/{}"
TD_SPARK_JAR_NAME = "td-spark-assembly_2.11-1.1.0.jar"


class Table(object):
    """A table writer module that imports Python data to a table.

    Parameters
    ----------
    client : pytd.Client
        Treasure Data client.

    database : string
        Database name.

    table : string
        Table name.
    """

    def __init__(self, client, database, table):
        try:
            client.api_client.database(database)
        except tdclient.errors.NotFoundError as e:
            raise ValueError(
                "faild to create pytd.table.Table instance for `{}.{}`: {}".format(
                    database, table, e
                )
            )

        self.database = database
        self.table = table
        self.client = client

    @property
    def exist(self):
        """Check if a configured table exists.

        Returns
        -------
        boolean
        """
        try:
            self.client.api_client.table(self.database, self.table)
        except tdclient.errors.NotFoundError:
            return False
        return True

    def create(self, column_names=[], column_types=[]):
        """Create a table named as configured.

        When ``column_names`` and ``column_types`` are given, table is created
        by a Presto query with the specified schema.

        Parameters
        ----------
        column_names : list of string, optional
            Column names.

        column_types : list of string, optional
            Column types corresponding to the names. Note that Treasure Data
            supports limited amount of types as documented in:
            https://support.treasuredata.com/hc/en-us/articles/360001266468-Schema-Management
        """
        if len(column_names) > 0:
            schema = ", ".join(
                map(
                    lambda t: "{} {}".format(t[0], t[1]),
                    zip(column_names, column_types),
                )
            )
            q_create = "CREATE TABLE {}.{} ({})".format(
                self.database, self.table, schema
            )
            self.client.query(q_create, engine="presto")
        else:
            self.client.api_client.create_log_table(self.database, self.table)

    def delete(self):
        """Delete a table from Treasure Data.
        """
        self.client.api_client.delete_table(self.database, self.table)

    def import_dataframe(self, dataframe, writer, if_exists="error", **kwargs):
        """Import a given DataFrame to a Treasure Data table.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data loaded to a target table.

        writer : string, {'bulk_import', 'insert_into', 'spark'}, or \
                    pytd.writer.Writer
            A Writer to choose writing method to Treasure Data. If string
            value, a temporal Writer instance will be created.

        if_exists : {'error', 'overwrite', 'append', 'ignore'}, default: 'error'
            What happens when a target table already exists.
        """
        # normalize column names so it contains only alphanumeric and `_`
        dataframe = dataframe.rename(
            lambda c: re.sub(r"[^a-zA-Z0-9]", " ", str(c)).lower().replace(" ", "_"),
            axis="columns",
        )

        writer_from_string = isinstance(writer, str)

        if writer_from_string:
            writer = Writer.from_string(writer, **kwargs)

        writer.write_dataframe(dataframe, self, if_exists)

        if writer_from_string:
            writer.close()
