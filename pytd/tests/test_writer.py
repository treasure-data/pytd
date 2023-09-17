import io
import os
import unittest
from unittest.mock import ANY, MagicMock

import numpy as np
import pandas as pd

from pytd.writer import (
    BulkImportWriter,
    InsertIntoWriter,
    SparkWriter,
    _cast_dtypes,
    _get_schema,
    _isinstance_or_null,
    _to_list,
)


class WriterTestCase(unittest.TestCase):
    def setUp(self):
        self.dft = pd.DataFrame(
            {
                "A": np.random.rand(3),
                "B": 1,
                "C": "foo",
                "D": pd.Timestamp("20010102"),
                "E": pd.Series([1.0] * 3).astype("float32"),
                "F": False,
                "G": pd.Series([1] * 3, dtype="int8"),
                "H": [[0, None, 2], [1, 2, 3], None],
                "I": [np.array([0, np.nan, 2]), np.array([1, 2, 3]), np.nan],
                "J": [np.array([0, np.nan, 2]), [1, 2, 3], None],
                "K": [True, np.nan, False],
                "L": [True, None, False],
                "M": ["foo", None, "bar"],
                "N": [1, None, 3],
                "O": pd.Series([1, 2, None], dtype="Int64"),
            }
        )

    def test_to_list(self):
        self.assertIsNone(_to_list(np.nan))
        self.assertIsNone(_to_list(None))
        # 0-d array becomes None
        self.assertIsNone(_to_list(np.array(None)))
        self.assertIsNone(_to_list(np.array(np.nan)))
        self.assertEqual(_to_list([None]), [None])
        self.assertEqual(_to_list([np.nan]), [None])
        self.assertEqual(_to_list(np.array([None])), [None])
        self.assertEqual(_to_list(np.array([np.nan])), [None])
        self.assertEqual(_to_list([None, None, None]), [None, None, None])
        self.assertEqual(_to_list([np.nan, np.nan, np.nan]), [None, None, None])
        self.assertEqual(_to_list(np.array([None, None, None])), [None, None, None])
        self.assertEqual(
            _to_list(np.array([np.nan, np.nan, np.nan])), [None, None, None]
        )
        self.assertEqual(_to_list([1, 2, np.nan]), [1, 2, None])
        self.assertEqual(_to_list(np.array([1, 2, np.nan])), [1.0, 2.0, None])
        self.assertEqual(
            _to_list(np.array(["foo", "bar", np.nan])), ["foo", "bar", None]
        )
        # String "nan" is converted into None since np.nan in Unicode dtype forces to
        # convert it into "nan"
        self.assertEqual(
            _to_list(np.array(["foo", "bar", "nan"])), ["foo", "bar", None]
        )

    def test_cast_dtypes(self):
        dft = _cast_dtypes(self.dft, inplace=False)
        dtypes = set(dft.dtypes)
        self.assertEqual(
            dtypes,
            set([np.dtype("int64"), np.dtype("float"), np.dtype("O"), pd.Int64Dtype()]),
        )
        self.assertEqual(dft["F"][0], "false")
        self.assertTrue(isinstance(dft["H"][1], str))
        self.assertEqual(dft["H"][1], "[1, 2, 3]")
        self.assertIsNone(dft["H"][2])
        self.assertIsNone(dft["I"][2])
        self.assertIsNone(dft["J"][2])
        self.assertIsNone(dft["K"][1])
        self.assertIsNone(dft["L"][1])
        self.assertIsNone(dft["M"][1])
        self.assertTrue(np.isnan(dft["N"][1]))
        # Nullable int will be float dtype by pandas default
        self.assertTrue(isinstance(dft["N"][0], float))
        # _cast_dtypes keeps np.nan/pd.NA when None in Int64 column given
        # This is for consistency of _get_schema
        self.assertTrue(pd.isna(dft["O"][2]))

    @unittest.skipIf(
        pd.__version__ < "1.0.0", "pd.NA is not supported in this pandas version"
    )
    def test_cast_dtypes_nullable(self):
        dft = pd.DataFrame(
            {
                "P": pd.Series([True, False, None], dtype="boolean"),
                "Q": pd.Series(["foo", "bar", None], dtype="string"),
            }
        )

        dft = _cast_dtypes(dft, inplace=False)
        dtypes = set(dft.dtypes)
        self.assertEqual(dtypes, set([np.dtype("O")]))
        self.assertIsNone(dft["P"][2])
        self.assertIsNone(dft["Q"][2])

    def test_cast_dtypes_inplace(self):
        _cast_dtypes(self.dft)
        dtypes = set(self.dft.dtypes)
        self.assertEqual(
            dtypes,
            set([np.dtype("int64"), np.dtype("float"), np.dtype("O"), pd.Int64Dtype()]),
        )
        self.assertEqual(self.dft["F"][0], "false")

    def test_cast_dtypes_keep_list(self):
        _cast_dtypes(self.dft, keep_list=True)
        dtypes = set(self.dft.dtypes)
        self.assertEqual(
            dtypes,
            set([np.dtype("int64"), np.dtype("float"), np.dtype("O"), pd.Int64Dtype()]),
        )
        self.assertTrue(self.dft["H"].apply(_isinstance_or_null, args=(list,)).all())
        self.assertTrue(self.dft["I"].apply(_isinstance_or_null, args=(list,)).all())
        self.assertTrue(self.dft["J"].apply(_isinstance_or_null, args=(list,)).all())
        self.assertTrue(isinstance(self.dft["H"].iloc[0][2], int))
        # numpy.ndarray containing numpy.nan will be converted as float type
        self.assertTrue(isinstance(self.dft["I"].iloc[0][2], float))
        self.assertTrue(isinstance(self.dft["I"].iloc[1][2], int))
        self.assertIsNone(self.dft["I"].iloc[0][1])


class InsertIntoWriterTestCase(unittest.TestCase):
    def setUp(self):
        self.writer = InsertIntoWriter()
        self.table = MagicMock()
        self.table.exists.return_value = True

    def test_write_dataframe_error(self):
        with self.assertRaises(RuntimeError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, "error"
            )

    def test_write_dataframe_ignore(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), self.table, "ignore"
        )
        self.assertFalse(self.table.client.query.called)

    def test_write_dataframe_append(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), self.table, "append"
        )
        # 1) INSERT INTO
        self.assertEqual(self.table.client.query.call_count, 1)

    def test_write_dataframe_overwrite(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), self.table, "overwrite"
        )
        # 1) Delete existing table
        self.assertEqual(self.table.delete.call_count, 1)
        # 2) Create an alternative table
        self.assertEqual(self.table.create.call_count, 1)
        # 3) INSERT INTO
        self.assertEqual(self.table.client.query.call_count, 1)

    def test_write_dataframe_invalid_if_exists(self):
        with self.assertRaises(ValueError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, if_exists="bar"
            )

    def test_schema(self):
        df = pd.DataFrame(
            {"a": [1, 2], "b": [None, 3], "c": pd.array([4, np.nan], dtype="Int64")}
        )
        _cast_dtypes(df)
        names, types = _get_schema(df)
        self.assertListEqual(names, ["a", "b", "c"])
        self.assertListEqual(types, ["bigint", "double", "bigint"])

    def test_query_builder(self):
        df = pd.DataFrame(
            {"a": [1, 2], "b": [None, 3], "c": pd.array([4, np.nan], dtype="Int64")}
        )
        _cast_dtypes(df)
        q = self.writer._build_query(
            "foo", "bar", list(df.itertuples(index=False, name=None)), df.columns
        )
        # column 'b' is handled as float64 because of null
        q_expected = "INSERT INTO foo.bar (a, b, c) VALUES (1, null, 4), (2, 3.0, null)"
        self.assertEqual(q, q_expected)

    def test_close(self):
        self.writer.close()
        self.assertTrue(self.writer.closed)
        with self.assertRaises(RuntimeError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, "error"
            )

    def test_table(self):
        with self.assertRaises(TypeError):
            self.writer.write_dataframe(pd.DataFrame([[1, 2], [3, 4]]), "foo", "error")


class BulkImportWriterTestCase(unittest.TestCase):
    def setUp(self):
        self.writer = BulkImportWriter()

        mock_bulk_import = MagicMock()
        mock_bulk_import.error_records = 1
        mock_bulk_import.valid_records = 2
        mock_bulk_import.upload_part.return_value = MagicMock()
        mock_bulk_import.upload_file.return_value = MagicMock()

        mock_api_client = MagicMock()
        mock_api_client.create_bulk_import.return_value = mock_bulk_import

        self.table = MagicMock()
        self.table.client.api_client = mock_api_client
        self.table.exists.return_value = True

    def test_write_dataframe_error(self):
        with self.assertRaises(RuntimeError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, "error"
            )

    def test_write_dataframe_ignore(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), self.table, "ignore"
        )
        self.assertFalse(self.table.client.api_client.create_bulk_import.called)

    def test_write_dataframe_append(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), self.table, "append"
        )
        self.assertTrue(self.table.client.api_client.create_bulk_import.called)
        args, kwargs = self.table.client.api_client.create_bulk_import.call_args
        self.assertEqual(kwargs.get("params"), {"mode": "append"})

    def test_write_dataframe_overwrite(self):
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), self.table, "overwrite"
        )
        self.assertTrue(self.table.delete.called)
        self.assertTrue(self.table.create.called)
        self.assertTrue(self.table.client.api_client.create_bulk_import.called)
        args, kwargs = self.table.client.api_client.create_bulk_import.call_args
        self.assertEqual(kwargs.get("params"), None)

    def test_write_dataframe_tempfile_deletion(self):
        # Case #1: bulk import succeeded
        self.writer._bulk_import = MagicMock()
        self.writer.write_dataframe(
            pd.DataFrame([[1, 2], [3, 4]]), self.table, "overwrite"
        )
        # file pointer to a temp CSV file
        fp = self.writer._bulk_import.call_args[0][1]
        # temp file should not exist
        self.assertFalse(os.path.isfile(fp[0].name))

        # Case #2: bulk import failed
        self.writer._bulk_import = MagicMock(side_effect=Exception())
        with self.assertRaises(Exception):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, "overwrite"
            )
        fp = self.writer._bulk_import.call_args[0][1]
        self.assertFalse(os.path.isfile(fp[0].name))

    def test_write_dataframe_msgpack(self):
        df = pd.DataFrame([[1, 2], [3, 4]])
        self.writer.write_dataframe(df, self.table, "overwrite", fmt="msgpack")
        api_client = self.table.client.api_client
        self.assertTrue(api_client.create_bulk_import.called)
        self.assertTrue(api_client.create_bulk_import().upload_part.called)
        _bytes = BulkImportWriter()._write_msgpack_stream(
            df.to_dict(orient="records"), io.BytesIO()
        )
        size = _bytes.getbuffer().nbytes
        api_client.create_bulk_import().upload_part.assert_called_with(
            "part-0", ANY, size
        )
        self.assertFalse(api_client.create_bulk_import().upload_file.called)

    def test_write_dataframe_msgpack_with_int_na(self):
        # Although this conversion ensures pd.NA Int64 dtype to None,
        # BulkImport API will treat the column as varchar
        df = pd.DataFrame(
            data=[
                {"a": 1, "b": 2, "time": 1234},
                {"a": 3, "b": 4, "c": 5, "time": 1234},
            ],
            dtype="Int64",
        )
        expected_list = (
            {"a": 1, "b": 2, "c": None, "time": 1234},
            {"a": 3, "b": 4, "c": 5, "time": 1234},
        )
        self.writer._write_msgpack_stream = MagicMock()
        self.writer.write_dataframe(df, self.table, "overwrite", fmt="msgpack")
        self.assertTrue(self.writer._write_msgpack_stream.called)
        print(self.writer._write_msgpack_stream.call_args[0][0][0:2])
        self.assertEqual(
            self.writer._write_msgpack_stream.call_args[0][0][0:2],
            expected_list,
        )

    @unittest.skipIf(
        pd.__version__ < "1.0.0", "pd.NA not supported in this pandas version"
    )
    def test_write_dataframe_msgpack_with_string_na(self):
        df = pd.DataFrame(
            data=[{"a": "foo", "b": "bar"}, {"a": "buzz", "b": "buzz", "c": "alice"}],
            dtype="string",
        )
        df["time"] = 1234
        expected_list = (
            {"a": "foo", "b": "bar", "c": None, "time": 1234},
            {"a": "buzz", "b": "buzz", "c": "alice", "time": 1234},
        )
        self.writer._write_msgpack_stream = MagicMock()
        self.writer.write_dataframe(df, self.table, "overwrite", fmt="msgpack")
        self.assertTrue(self.writer._write_msgpack_stream.called)
        self.assertEqual(
            self.writer._write_msgpack_stream.call_args[0][0][0:2],
            expected_list,
        )

    @unittest.skipIf(
        pd.__version__ < "1.0.0", "pd.NA not supported in this pandas version"
    )
    def test_write_dataframe_msgpack_with_boolean_na(self):
        df = pd.DataFrame(
            data=[{"a": True, "b": False}, {"a": False, "b": True, "c": True}],
            dtype="boolean",
        )
        df["time"] = 1234
        expected_list = (
            {"a": "true", "b": "false", "c": None, "time": 1234},
            {"a": "false", "b": "true", "c": "true", "time": 1234},
        )
        self.writer._write_msgpack_stream = MagicMock()
        self.writer.write_dataframe(df, self.table, "overwrite", fmt="msgpack")
        self.assertTrue(self.writer._write_msgpack_stream.called)
        self.assertEqual(
            self.writer._write_msgpack_stream.call_args[0][0][0:2],
            expected_list,
        )

    def test_write_dataframe_invalid_if_exists(self):
        with self.assertRaises(ValueError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, if_exists="bar"
            )

    def test_close(self):
        self.writer.close()
        self.assertTrue(self.writer.closed)
        with self.assertRaises(RuntimeError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, "error"
            )

    def test_table(self):
        with self.assertRaises(TypeError):
            self.writer.write_dataframe(pd.DataFrame([[1, 2], [3, 4]]), "foo", "error")


class SparkWriterTestCase(unittest.TestCase):
    def setUp(self):
        self.writer = SparkWriter()

        td_spark = MagicMock()
        td_spark.spark._jsc.sc().isStopped.return_value = False
        self.writer.td_spark = td_spark
        self.writer.fetched_apikey = "1/XXX"
        self.writer.fetched_endpoint = "ENDPOINT"

        self.table = MagicMock()
        self.table.client.apikey = "1/XXX"
        self.table.client.endpoint = "ENDPOINT"

    def test_write_dataframe(self):
        df = pd.DataFrame([[1, 2], [3, 4]])
        self.writer.write_dataframe(df, self.table, "overwrite")
        self.assertTrue(self.writer.td_spark.spark.createDataFrame.called)

    def test_write_dataframe_invalid_if_exists(self):
        with self.assertRaises(ValueError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, if_exists="bar"
            )

    def test_write_dataframe_with_int_na(self):
        df = pd.DataFrame(
            data=[{"a": 1, "b": 2}, {"a": 3, "b": 4, "c": 5}], dtype="Int64"
        )
        expected_df = df.replace({np.nan: None})
        for col in ["a", "b"]:
            expected_df[col] = expected_df[col].astype("int64")
        self.writer.td_spark.spark.createDataFrame.return_value = "Dummy DataFrame"
        self.writer.write_dataframe(df, self.table, "overwrite")
        pd.testing.assert_frame_equal(
            self.writer.td_spark.spark.createDataFrame.call_args[0][0], expected_df
        )

    @unittest.skipIf(
        pd.__version__ < "1.0.0", "pd.NA is not supported in this pandas version"
    )
    def test_write_dataframe_with_string_na(self):
        df = pd.DataFrame(
            data=[{"a": "foo", "b": "bar"}, {"a": "buzz", "b": "buzz", "c": "alice"}],
            dtype="string",
        )
        expected_df = df.replace({np.nan: None}).astype(object)
        self.writer.td_spark.spark.createDataFrame.return_value = "Dummy DataFrame"
        self.writer.write_dataframe(df, self.table, "overwrite")
        pd.testing.assert_frame_equal(
            self.writer.td_spark.spark.createDataFrame.call_args[0][0], expected_df
        )

    @unittest.skipIf(
        pd.__version__ < "1.0.0", "pd.NA is not supported in this pandas version"
    )
    def test_write_dataframe_with_boolean_na(self):
        df = pd.DataFrame(
            data=[{"a": True, "b": False}, {"a": False, "b": True, "c": True}],
            dtype="boolean",
        )
        expected_df = pd.DataFrame(
            data=[{"a": "true", "b": "false"}, {"a": "false", "b": "true", "c": "true"}]
        )
        self.writer.td_spark.spark.createDataFrame.return_value = "Dummy DataFrame"
        self.writer.write_dataframe(df, self.table, "overwrite")
        pd.testing.assert_frame_equal(
            self.writer.td_spark.spark.createDataFrame.call_args[0][0], expected_df
        )

    def test_close(self):
        self.writer.close()
        self.assertTrue(self.writer.td_spark.spark.stop.called)

        self.writer.td_spark.spark._jsc.sc().isStopped.return_value = True
        self.assertTrue(self.writer.closed)
        with self.assertRaises(RuntimeError):
            self.writer.write_dataframe(
                pd.DataFrame([[1, 2], [3, 4]]), self.table, "error"
            )

    def test_table(self):
        with self.assertRaises(TypeError):
            self.writer.write_dataframe(pd.DataFrame([[1, 2], [3, 4]]), "foo", "error")
