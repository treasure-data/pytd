"""IPython Magics

    IPython magics to access to Treasure Data. Load the magics first of all:

    .. code-block:: ipython

        In [1]: %load_ext pytd.pandas_td.ipython
"""

import argparse
import os
import re
import sys

import numpy as np
import pandas as pd
import pytz
import tdclient
from IPython import display, get_ipython
from IPython.core import magic

from . import connect, create_engine, read_td_job, read_td_query

MAGIC_CONTEXT_NAME = "_td_magic"


class MagicContext(object):
    def __init__(self):
        self.database = None

    def connect(self):
        return connect()


class MagicTable(object):
    def __init__(self, table):
        print(f"INFO: import {table.name}")
        self.table = table
        data = [c if len(c) == 3 else [c[0], c[1], ""] for c in table.schema]
        self.columns = [c[2] if c[2] else c[0] for c in data]
        self.frame = pd.DataFrame(data, columns=["field", "type", "alias"])

    def __dir__(self):
        return self.columns

    def _repr_html_(self):
        return self.frame._repr_html_()


def get_td_magic_context():
    ipython = get_ipython()
    try:
        ctx = ipython.ev(MAGIC_CONTEXT_NAME)
    except NameError:
        ctx = MagicContext()
        ipython.push({MAGIC_CONTEXT_NAME: ctx})
    return ctx


class TDMagics(magic.Magics):
    def __init__(self, shell):
        super(TDMagics, self).__init__(shell)
        self.context = get_td_magic_context()


@magic.magics_class
class DatabasesMagics(TDMagics):
    @magic.line_magic
    def td_databases(self, pattern):
        """List databases in the form of pandas.DataFrame.

        .. code-block:: python

            %td_databases [<database_name_pattern>]

        Parameters
        ----------
        ``<database_name_pattern>`` : string, optional
            List databases matched to a given pattern. If not given, all existing
            databases will be listed.

        Returns
        -------
        :class:`pandas.DataFrame`

        Examples
        --------
        .. code-block:: ipython

            In [1]: %load_ext pytd.pandas_td.ipython

            In [2]: %td_databases sample
            Out[2]:
                                                name        count     permission                created_at                updated_at
            0    xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx       348124  administrator 2019-01-23 05:48:11+00:00 2019-01-23 05:48:11+00:00
            1                              yyyyyyyyy            0  administrator 2017-12-14 07:52:34+00:00 2017-12-14 07:52:34+00:00
            2                          zzzzzzzzzzzzz            0  administrator 2016-05-25 23:12:06+00:00 2016-05-25 23:12:06+00:00
            ...

            In [3]: %td_databases sample
            Out[3]:
                                     name     count     permission                created_at                updated_at
            0                    sampledb         2  administrator 2014-04-11 22:29:38+00:00 2014-04-11 22:29:38+00:00
            1             sample_xxxxxxxx         2  administrator 2017-06-02 23:37:41+00:00 2017-06-02 23:37:41+00:00
            2             sample_datasets   8812278     query_only 2014-10-04 01:13:11+00:00 2018-03-16 04:59:06+00:00
            ...
        """
        con = self.context.connect()
        columns = ["name", "count", "permission", "created_at", "updated_at"]
        values = [
            [getattr(db, c) for c in columns]
            for db in con.list_databases()
            if re.search(pattern, db.name)
        ]
        return pd.DataFrame(values, columns=columns)


@magic.magics_class
class TablesMagics(TDMagics):
    @magic.line_magic
    def td_tables(self, pattern):
        """List tables in databases.

        .. code-block:: python

            %td_tables [<table_identifier_pattern>]

        Parameters
        ----------
        ``<table_identifier_pattern>`` : string, optional
            List tables matched to a given pattern. Table identifier is
            represented as ``database_name.table_name``. If not given, all
            existing tables will be listed.

        Returns
        -------
        :class:`pandas.DataFrame`

        Examples
        --------
        .. code-block:: ipython

            In [1]: %load_ext pytd.pandas_td.ipython

            In [2]: %td_tables
            Out[2]:
                        db_name                         name      count  estimated_storage_size        last_log_timestamp                created_at
            0     xxxxx_demo_aa                customer_test         70                    1047 2018-02-05 06:20:32+00:00 2018-02-05 06:20:24+00:00
            1     xxxxx_demo_aa                    email_log          0                       0 1970-01-01 00:00:00+00:00 2018-02-05 07:19:57+00:00
            2             yy_wf           topk_similar_items      10598                  134208 2018-04-16 09:23:57+00:00 2018-04-16 09:59:48+00:00
            ...

            In [3]: %td_tables sample
            Out[3]:
                              db_name                                 name    count  estimated_storage_size        last_log_timestamp                created_at
            0                 xx_test                      aaaaaaaa_sample        0                       0 1970-01-01 00:00:00+00:00 2015-10-20 17:37:40+00:00
            1                sampledb                            sampletbl        2                     843 1970-01-01 00:00:00+00:00 2014-04-11 22:30:08+00:00
            2            zzzz_test_db                    sample_output_tab        4                     889 2018-06-06 08:26:20+00:00 2018-06-06 08:27:12+00:00
            ...
        """
        con = self.context.connect()
        columns = [
            "db_name",
            "name",
            "count",
            "estimated_storage_size",
            "last_log_timestamp",
            "created_at",
        ]
        values = [
            [getattr(t, c) for c in columns]
            for db in con.list_databases()
            for t in con.list_tables(db.name)
            if re.search(pattern, t.identifier)
        ]
        return pd.DataFrame(values, columns=columns)


@magic.magics_class
class JobsMagics(TDMagics):
    @magic.line_magic
    def td_jobs(self, line):
        """List job activities in an account.

        .. code-block:: python

            %td_jobs

        Returns
        -------
        :class:`pandas.DataFrame`

        Examples
        --------
        .. code-block:: ipython

            In [1]: %load_ext pytd.pandas_td.ipython

            In [2]: %td_jobs
            Out[2]:
                 status     job_id    type                  start_at                                              query
            0     error  448650806    hive 2019-04-12 05:33:36+00:00  with null_samples as (\\n  select\\n    id,\\n   ...
            1   success  448646994  presto 2019-04-12 05:23:29+00:00  -- read_td_query\\n-- set session distributed_j...
            2   success  448646986  presto 2019-04-12 05:23:27+00:00  -- read_td_query\\n-- set session distributed_j...
            ...
        """
        con = self.context.connect()
        columns = ["status", "job_id", "type", "start_at", "query"]
        values = [
            [j.status(), j.job_id, j.type, j._start_at, j.query]
            for j in con.list_jobs()
        ]
        return pd.DataFrame(values, columns=columns)


@magic.magics_class
class UseMagics(TDMagics):
    @magic.line_magic
    def td_use(self, line):
        """Use a specific database.

        This magic pushes all table names in a specified database into the
        current namespace.

        .. code-block:: python

            %td_use [<database_name>]

        Parameters
        ----------
        ``<database_name>`` : string
            Database name.

        Examples
        --------
        .. code-block:: ipython

            In [1]: %load_ext pytd.pandas_td.ipython

            In [2]: %td_use sample_datasets
            INFO: import nasdaq
            INFO: import www_access

            In [3]: nasdaq  # describe table columns in the form of DataFrame
            Out[3]: <pytd.pandas_td.ipython.MagicTable at 0x117651908>
        """
        con = self.context.connect()
        try:
            tables = con.list_tables(line)
        except tdclient.api.NotFoundError:
            sys.stderr.write(f"ERROR: Database '{line}' not found.")
            return
        # update context
        self.context.database = line
        # push table names
        get_ipython().push({t.name: MagicTable(t) for t in tables})


@magic.magics_class
class QueryMagics(TDMagics):
    def create_job_parser(self):
        parser = argparse.ArgumentParser(
            prog="job", description="Line magic to get job result.", add_help=False
        )
        parser.add_argument("job_id", type=int, help="job ID")
        parser.add_argument(
            "--pivot", action="store_true", help="run pivot_table against dimensions"
        )
        parser.add_argument("--plot", action="store_true", help="plot the query result")
        parser.add_argument(
            "-n",
            "--dry-run",
            action="store_true",
            help="output translated code without running query",
        )
        parser.add_argument(
            "-v", "--verbose", action="store_true", help="verbose output"
        )
        parser.add_argument("-c", "--connection", help="use specified connection")
        parser.add_argument(
            "-d",
            "--dropna",
            action="store_true",
            help="drop columns if all values are NA",
        )
        parser.add_argument("-o", "--out", help="store the result to variable")
        parser.add_argument("-O", "--out-file", help="store the result to file")
        parser.add_argument(
            "-q", "--quiet", action="store_true", help="disable progress output"
        )
        parser.add_argument("-T", "--timezone", help="set timezone to time index")
        return parser

    def parse_job_args(self, line):
        parser = self.create_job_parser()
        args = parser.parse_args(line.split())

        # validate timezone
        if args.timezone:
            pytz.timezone(args.timezone)

        # implicit options
        if args.plot:
            args.pivot = True

        return args

    def create_query_parser(self, engine_type):
        parser = argparse.ArgumentParser(
            prog=engine_type, description="Cell magic to run a query.", add_help=False
        )
        parser.add_argument("database", nargs="?", help="database name")
        parser.add_argument(
            "--pivot", action="store_true", help="run pivot_table against dimensions"
        )
        parser.add_argument("--plot", action="store_true", help="plot the query result")
        parser.add_argument(
            "-n",
            "--dry-run",
            action="store_true",
            help="output translated code without running query",
        )
        parser.add_argument(
            "-v", "--verbose", action="store_true", help="verbose output"
        )
        parser.add_argument("-c", "--connection", help="use specified connection")
        parser.add_argument(
            "-d",
            "--dropna",
            action="store_true",
            help="drop columns if all values are NA",
        )
        parser.add_argument("-o", "--out", help="store the result to variable")
        parser.add_argument("-O", "--out-file", help="store the result to file")
        parser.add_argument(
            "-q", "--quiet", action="store_true", help="disable progress output"
        )
        parser.add_argument("-T", "--timezone", help="set timezone to time index")
        return parser

    def parse_query_args(self, engine_type, line):
        parser = self.create_query_parser(engine_type)
        args = parser.parse_args(line.split())

        # validate timezone
        if args.timezone:
            pytz.timezone(args.timezone)

        # implicit options
        if args.plot:
            args.pivot = True

        # context
        if args.database is None:
            args.database = self.context.database

        return args

    def push_code(self, code, end="\n"):
        self.code_list.append(code + end)

    def display_code_block(self):
        html = '<pre style="background-color: #ffe;">'
        html += "".join(self.code_list)
        html += "</pre>\n"
        display.display(display.HTML(html))

    def build_query(self, cell):
        ip = get_ipython()
        query = cell.format(**ip.user_ns)
        self.push_code("_q = '''")
        self.push_code(query)
        self.push_code("'''")
        return query

    def build_engine(self, engine_type, database, args):
        ip = get_ipython()
        name = f"{engine_type}:{database}"
        code_args = [repr(name)]
        # connection
        if args.connection:
            con = ip.ev(args.connection)
            code_args.append(f"con={args.connection}")
        else:
            con = self.context.connect()
        # engine
        if args.quiet:
            params = {"show_progress": False, "clear_progress": False}
        elif args.verbose:
            params = {"show_progress": True, "clear_progress": False}
        else:
            params = {}
        code_args += [f"{k}={v}" for k, v in params.items()]
        self.push_code(f"_e = pytd.pandas_td.create_engine({', '.join(code_args)})")
        return create_engine(name, con=con, **params)

    def convert_time(self, d):
        if "time" in d.columns:
            if d["time"].dtype == np.dtype("O"):
                self.push_code("_d['time'] = pd.to_datetime(_d['time'])")
                d["time"] = pd.to_datetime(d["time"])
            else:
                self.push_code("_d['time'] = pd.to_datetime(_d['time'], unit='s')")
                d["time"] = pd.to_datetime(d["time"], unit="s")

    def set_index(self, d, index, args):
        self.push_code(f"_d.set_index({repr(index)}, inplace=True)")
        d.set_index(index, inplace=True)
        if index == "time" and args.timezone:
            self.push_code("_d.tz_localize('UTC', copy=False)")
            self.push_code(f"_d.tz_convert('{args.timezone}', copy=False)")
            d.tz_localize("UTC", copy=False).tz_convert(args.timezone, copy=False)

    def pivot(self, d, args):
        def is_dimension(c, t):
            return c.endswith("_id") or t == np.dtype("O")

        index = d.columns[0]
        dimension = [
            c for c, t in zip(d.columns[1:], d.dtypes[1:]) if is_dimension(c, t)
        ]
        measure = [
            c for c, t in zip(d.columns[1:], d.dtypes[1:]) if not is_dimension(c, t)
        ]
        if len(dimension) == 0:
            self.set_index(d, index, args)
            return d
        if len(dimension) == 1:
            dimension = dimension[0]
        if len(measure) == 1:
            measure = measure[0]
        self.push_code(
            f"_d = _d.pivot({repr(index)}, {repr(dimension)}, {repr(measure)})"
        )
        return d.pivot(index, dimension, measure)

    def post_process(self, d, args):
        ip = get_ipython()

        # convert 'time' to datetime
        self.convert_time(d)

        # dropna by columns all
        if args.dropna:
            self.push_code("_d.dropna(axis='columns', how='all', inplace=True)")
            d.dropna(axis="columns", how="all", inplace=True)

        # pivot_table
        if args.pivot:
            d = self.pivot(d, args)
        elif "time" in d.columns:
            self.set_index(d, "time", args)

        # return value
        r = d
        if args.out:
            self.push_code(f"{args.out} = _d")
            ip.push({args.out: d})
            r = None
        if args.out_file:
            if args.out_file[0] in ["'", '"']:
                path = os.path.expanduser(ip.ev(args.out_file))
            else:
                path = os.path.expanduser(args.out_file)
            if d.index.name:
                self.push_code(f"_d.to_csv({repr(path)})")
                d.to_csv(path)
            else:
                self.push_code(f"_d.to_csv({repr(path)}, index=False)")
                d.to_csv(path, index=False)
            print(f"INFO: saved to '{path}'")
            r = None
        if args.plot:
            self.push_code("_d.plot()")
            r = d.plot()
        elif r is not None:
            self.push_code("_d")
        return r

    def run_job(self, line):
        ip = get_ipython()

        try:
            args = self.parse_job_args(line)
        except SystemExit:
            return

        self.code_list = []
        self.push_code("# translated code")
        if args.connection:
            con = ip.ev(args.connection)
        else:
            con = self.context.connect()

        # engine
        job = con.get_job(args.job_id)
        engine = self.build_engine(job.type, job.database, args)

        # read_td_query
        self.push_code(f"_d = pytd.pandas_td.read_td_job({args.job_id}, _e)")
        if args.dry_run:
            return self.display_code_block()
        d = read_td_job(args.job_id, engine)

        # output
        r = self.post_process(d, args)
        if args.verbose:
            self.display_code_block()
        return r

    def run_query(self, engine_type, line, cell):
        try:
            args = self.parse_query_args(engine_type, line)
        except SystemExit:
            return

        self.code_list = []
        self.push_code("# translated code")
        query = self.build_query(cell)
        engine = self.build_engine(engine_type, args.database, args)

        # read_td_query
        self.push_code("_d = pytd.pandas_td.read_td_query(_q, _e)")
        if args.dry_run:
            return self.display_code_block()
        d = read_td_query(query, engine)

        # output
        r = self.post_process(d, args)
        if args.verbose:
            self.display_code_block()
        return r

    @magic.line_magic
    def td_job(self, line):
        """Get job result.

        .. code-block:: python

            %td_job [--pivot] [--plot] [--dry-run] [--verbose]
                    [--connection <connection>] [--dropna] [--out <out>]
                    [--out-file <out_file>] [--quiet] [--timezone <timezone>]
                    job_id

        Parameters
        ----------
        ``<job_id>`` : integer
            Job ID.

        ``--pivot`` : optional
            Run pivot_table against dimensions.

        ``--plot`` : optional
            Plot the query result.

        ``--dry_run``, ``-n`` : optional
            Output translated code without running query.

        ``--verbose``, ``-v`` : optional
            Verbose output.

        ``--connection <connection>``, ``-c <connection>`` : \
                pytd.Client, optional
            Use specified connection.

        ``--dropna``, ``-d`` : optional
            Drop columns if all values are NA.

        ``--out <out>``, ``-o <out>`` : string, optional
            Store the result to variable.

        ``--out-file <out_file>``, ``-O <out_file>`` : string, optional
            Store the result to file.

        ``--quiet``, ``-q`` : optional
            Disable progress output.

        ``--timezone <timezone>``, ``-T <timezone>`` : string, optional
            Set timezone to time index.

        Returns
        -------
        :class:`pandas.DataFrame`

        Examples
        --------
        .. code-block:: ipython

            In [1]: %load_ext pytd.pandas_td.ipython

            In [2]: %td_job 451709460  # select * from sample_datasets.nasdaq limit 5
            Out[2]:
                                symbol  open  volume     high      low    close
            time
            1992-08-25 16:00:00   ATRO   0.0    3900   0.7076   0.7076   0.7076
            1992-08-25 16:00:00   ALOG   0.0   11200  11.0000  10.6250  11.0000
            1992-08-25 16:00:00   ATAX   0.0   11400  11.3750  11.0000  11.0000
            1992-08-25 16:00:00   ATRI   0.0    5400  14.3405  14.0070  14.2571
            1992-08-25 16:00:00   ABMD   0.0   38800   5.7500   5.2500   5.6875
        """
        return self.run_job(line)

    @magic.cell_magic
    def td_hive(self, line, cell):
        """Run a Hive query.

        .. code-block:: python

            %%td_hive [<database>] [--pivot] [--plot] [--dry-run] [--verbose]
                      [--connection <connection>] [--dropna] [--out <out>]
                      [--out-file <out_file>] [--quiet] [--timezone <timezone>]

            <query>

        Parameters
        ----------
        ``<query>`` : string
            Hive query.

        ``<database>`` : string, optional
            Database name.

        ``--pivot`` : optional
            Run pivot_table against dimensions.

        ``--plot`` : optional
            Plot the query result.

        ``--dry_run``, ``-n`` : optional
            Output translated code without running query.

        ``--verbose``, ``-v`` : optional
            Verbose output.

        ``--connection <connection>``, ``-c <connection>`` : \
                pytd.Client, optional
            Use specified connection.

        ``--dropna``, ``-d`` : optional
            Drop columns if all values are NA.

        ``--out <out>``, ``-o <out>`` : string, optional
            Store the result to variable.

        ``--out-file <out_file>``, ``-O <out_file>`` : string, optional
            Store the result to file.

        ``--quiet``, ``-q`` : optional
            Disable progress output.

        ``--timezone <timezone>``, ``-T <timezone>`` : string, optional
            Set timezone to time index.

        Returns
        -------
        :class:`pandas.DataFrame`

        Examples
        --------
        .. code-block:: ipython

            In [1]: %load_ext pytd.pandas_td.ipython

            In [2]: %%td_hive
               ...: select hivemall_version()
               ...:
            Out[2]:
                                     _c0
            0  0.6.0-SNAPSHOT-201901-r01
        """
        return self.run_query("hive", line, cell)

    @magic.cell_magic
    def td_presto(self, line, cell):
        """Run a Presto query.

        .. code-block:: python

            %%td_presto [<database>] [--pivot] [--plot] [--dry-run] [--verbose]
                        [--connection <connection>] [--dropna] [--out <out>]
                        [--out-file <out_file>] [--quiet] [--timezone <timezone>]

            <query>

        Parameters
        ----------
        ``<query>`` : string
            Presto query.

        ``<database>`` : string, optional
            Database name.

        ``--pivot`` : optional
            Run pivot_table against dimensions.

        ``--plot`` : optional
            Plot the query result.

        ``--dry_run``, ``-n`` : optional
            Output translated code without running query.

        ``--verbose``, ``-v`` : optional
            Verbose output.

        ``--connection <connection>``, ``-c <connection>`` : \
                pytd.Client, optional
            Use specified connection.

        ``--dropna``, ``-d`` : optional
            Drop columns if all values are NA.

        ``--out <out>``, ``-o <out>`` : string, optional
            Store the result to variable.

        ``--out-file <out_file>``, ``-O <out_file>`` : string, optional
            Store the result to file.

        ``--quiet``, ``-q`` : optional
            Disable progress output.

        ``--timezone <timezone>``, ``-T <timezone>`` : string, optional
            Set timezone to time index.

        Returns
        -------
        :class:`pandas.DataFrame`

        Examples
        --------
        .. code-block:: ipython

            In [1]: %load_ext pytd.pandas_td.ipython

            In [2]: %%td_presto
               ...: select * from sample_datasets.nasdaq limit 5
               ...:
            Out[2]:
                                symbol  open  volume     high      low    close
            time
            1989-01-26 16:00:00   SMTC   0.0    8000   0.4532   0.4532   0.4532
            1989-01-26 16:00:00   SEIC   0.0  163200   0.7077   0.6921   0.7025
            1989-01-26 16:00:00   SIGI   0.0    2800   3.9610   3.8750   3.9610
            1989-01-26 16:00:00   NAVG   0.0    1800  14.6740  14.1738  14.6740
            1989-01-26 16:00:00   MOCO   0.0   71101   3.6722   3.5609   3.5980
        """
        return self.run_query("presto", line, cell)


# extension
def load_ipython_extension(ipython):
    ipython.push("get_td_magic_context")
    ipython.register_magics(DatabasesMagics)
    ipython.register_magics(TablesMagics)
    ipython.register_magics(JobsMagics)
    ipython.register_magics(UseMagics)
    ipython.register_magics(QueryMagics)
