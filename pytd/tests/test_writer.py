import os
import tempfile
import unittest
from unittest.mock import ANY, MagicMock, patch

import msgpack
import numpy as np
import pandas as pd

from pytd.writer import (
    BulkImportWriter,
    InsertIntoWriter,
    SparkWriter,
    _cast_dtypes,
    _get_schema,
    _isinstance_or_null,
    _replace_pd_na,
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
                "P": pd.Series([1.0, 2.0, None], dtype="Float64"),
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
            set(
                [
                    np.dtype("int64"),
                    np.dtype("float64"),
                    np.dtype("O"),
                    pd.Int64Dtype(),
                    pd.Float64Dtype(),
                ]
            ),
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
        # N column is now Float64, so pd.NA instead of np.nan
        self.assertTrue(pd.isna(dft["N"][1]))
        # Nullable float will be Float64 dtype
        self.assertTrue(pd.api.types.is_extension_array_dtype(dft["N"]))
        # _cast_dtypes keeps np.nan/pd.NA when None in Int64 column given
        # This is for consistency of _get_schema
        self.assertTrue(pd.isna(dft["O"][2]))
        self.assertTrue(pd.isna(dft["P"][2]))

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
            set(
                [
                    np.dtype("int64"),
                    np.dtype("float64"),
                    np.dtype("O"),
                    pd.Int64Dtype(),
                    pd.Float64Dtype(),
                ]
            ),
        )
        self.assertEqual(self.dft["F"][0], "false")

    def test_cast_dtypes_keep_list(self):
        _cast_dtypes(self.dft, keep_list=True)
        dtypes = set(self.dft.dtypes)
        self.assertEqual(
            dtypes,
            set(
                [
                    np.dtype("int64"),
                    np.dtype("float64"),
                    np.dtype("O"),
                    pd.Int64Dtype(),
                    pd.Float64Dtype(),
                ]
            ),
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
        # column 'b' is handled as Float64 because of null, pd.NA becomes null
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

    def test_format_value_for_trino_special_values(self):
        """Test _format_value_for_trino with special floating-point values"""
        # Test infinity
        result = self.writer._format_value_for_trino(np.inf)
        self.assertEqual(result, "infinity()")

        # Test negative infinity
        result = self.writer._format_value_for_trino(-np.inf)
        self.assertEqual(result, "-infinity()")

        # Test nan
        result = self.writer._format_value_for_trino(np.nan)
        self.assertEqual(result, "nan()")

        # Test pd.NA, pd.NaT, None
        result = self.writer._format_value_for_trino(pd.NA)
        self.assertEqual(result, "null")

        result = self.writer._format_value_for_trino(pd.NaT)
        self.assertEqual(result, "null")

        result = self.writer._format_value_for_trino(None)
        self.assertEqual(result, "null")

    def test_format_value_for_trino_string_escaping(self):
        """Test _format_value_for_trino with string values requiring escaping"""
        result = self.writer._format_value_for_trino("hello")
        self.assertEqual(result, "'hello'")

        # Test string with single quotes - should be escaped
        result = self.writer._format_value_for_trino("O'Brien")
        self.assertEqual(result, "'O''Brien'")


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
        self.writer._bulk_import = MagicMock(side_effect=RuntimeError())
        with self.assertRaises(RuntimeError):
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
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            fp = BulkImportWriter()._write_msgpack_stream(
                df.to_dict(orient="records"), fp
            )
            api_client.create_bulk_import().upload_part.assert_called_with(
                "part-0", ANY, 62
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
        expected_list = [
            {"a": 1, "b": 2, "c": None, "time": 1234},
            {"a": 3, "b": 4, "c": 5, "time": 1234},
        ]
        self.writer._write_msgpack_stream = MagicMock()
        with patch("pytd.writer.os.unlink"):
            self.writer.write_dataframe(df, self.table, "overwrite", fmt="msgpack")
            self.assertTrue(self.writer._write_msgpack_stream.called)
            self.assertEqual(
                self.writer._write_msgpack_stream.call_args[0][0][0:2],
                expected_list,
            )

    def test_write_dataframe_msgpack_with_string_na(self):
        df = pd.DataFrame(
            data=[{"a": "foo", "b": "bar"}, {"a": "buzz", "b": "buzz", "c": "alice"}],
            dtype="string",
        )
        df["time"] = 1234
        expected_list = [
            {"a": "foo", "b": "bar", "c": None, "time": 1234},
            {"a": "buzz", "b": "buzz", "c": "alice", "time": 1234},
        ]
        self.writer._write_msgpack_stream = MagicMock()
        with patch("pytd.writer.os.unlink"):
            self.writer.write_dataframe(df, self.table, "overwrite", fmt="msgpack")
            self.assertTrue(self.writer._write_msgpack_stream.called)
            self.assertEqual(
                self.writer._write_msgpack_stream.call_args[0][0][0:2],
                expected_list,
            )

    def test_write_dataframe_msgpack_with_boolean_na(self):
        df = pd.DataFrame(
            data=[{"a": True, "b": False}, {"a": False, "b": True, "c": True}],
            dtype="boolean",
        )
        df["time"] = 1234
        expected_list = [
            {"a": "true", "b": "false", "c": None, "time": 1234},
            {"a": "false", "b": "true", "c": "true", "time": 1234},
        ]
        self.writer._write_msgpack_stream = MagicMock()
        with patch("pytd.writer.os.unlink"):
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

    def test_bulk_import_name_default(self):
        """Test that UUID-based session name is generated when bulk_import_name
        is not provided"""
        df = pd.DataFrame([[1, 2], [3, 4]])
        self.writer.write_dataframe(df, self.table, "overwrite")

        # Check that create_bulk_import was called
        self.assertTrue(self.table.client.api_client.create_bulk_import.called)
        args, kwargs = self.table.client.api_client.create_bulk_import.call_args

        # First argument should be the session name
        session_name = args[0]
        self.assertTrue(session_name.startswith("session-"))
        self.assertTrue(len(session_name) > len("session-"))  # Should have UUID part

    def test_bulk_import_name_custom(self):
        """Test that custom bulk_import_name is used when provided"""
        df = pd.DataFrame([[1, 2], [3, 4]])
        custom_name = "my-custom-import-job"

        self.writer.write_dataframe(
            df, self.table, "overwrite", bulk_import_name=custom_name
        )

        # Check that create_bulk_import was called with the custom name
        self.assertTrue(self.table.client.api_client.create_bulk_import.called)
        args, kwargs = self.table.client.api_client.create_bulk_import.call_args

        # First argument should be the custom session name
        session_name = args[0]
        self.assertEqual(session_name, custom_name)

    def test_bulk_import_name_custom_msgpack(self):
        """Test that custom bulk_import_name works with msgpack format"""
        df = pd.DataFrame([[1, 2], [3, 4]])
        custom_name = "my-msgpack-import"

        self.writer.write_dataframe(
            df, self.table, "overwrite", fmt="msgpack", bulk_import_name=custom_name
        )

        # Check that create_bulk_import was called with the custom name
        self.assertTrue(self.table.client.api_client.create_bulk_import.called)
        args, kwargs = self.table.client.api_client.create_bulk_import.call_args

        # First argument should be the custom session name
        session_name = args[0]
        self.assertEqual(session_name, custom_name)

    def test_commit_timeout_parameter(self):
        """Test that commit_timeout parameter is passed correctly"""
        df = pd.DataFrame([[1, 2], [3, 4]])
        timeout_value = 300  # 5 minutes

        # Mock the bulk_import.commit method to check if timeout is passed
        mock_bulk_import = self.table.client.api_client.create_bulk_import.return_value
        mock_bulk_import.commit = MagicMock()

        self.writer.write_dataframe(
            df, self.table, "overwrite", commit_timeout=timeout_value
        )

        # Check that commit was called with the timeout parameter
        mock_bulk_import.commit.assert_called_with(wait=True, timeout=timeout_value)

    def test_perform_wait_callback_parameter(self):
        """Test that perform_wait_callback parameter is passed correctly"""
        df = pd.DataFrame([[1, 2], [3, 4]])
        callback_func = MagicMock()

        # Mock the bulk_import.perform method to check if wait_callback is passed
        mock_bulk_import = self.table.client.api_client.create_bulk_import.return_value
        mock_bulk_import.perform = MagicMock()

        self.writer.write_dataframe(
            df, self.table, "overwrite", perform_wait_callback=callback_func
        )

        # Check that perform was called with the wait_callback parameter
        mock_bulk_import.perform.assert_called_with(
            wait=True, timeout=None, wait_callback=callback_func
        )

    def test_perform_timeout_parameter(self):
        """Test that perform_timeout parameter is passed correctly"""
        df = pd.DataFrame([[1, 2], [3, 4]])
        timeout_value = 300  # 5 minutes

        # Mock the bulk_import.perform method to check if timeout is passed
        mock_bulk_import = self.table.client.api_client.create_bulk_import.return_value
        mock_bulk_import.perform = MagicMock()

        self.writer.write_dataframe(
            df, self.table, "overwrite", perform_timeout=timeout_value
        )

        # Check that perform was called with the timeout parameter
        mock_bulk_import.perform.assert_called_with(
            wait=True, timeout=timeout_value, wait_callback=None
        )

    def test_msgpack_serialization_with_special_values(self):
        """Test that msgpack correctly handles values after special float processing"""

        # Create DataFrame with special values
        df = pd.DataFrame(
            {
                "float_col": [1.0, np.nan, np.inf, -np.inf, 2.5],
                "int_col": [1, 2, 3, 4, 5],
                "str_col": ["a", "b", "c", "d", "e"],
            }
        )

        # Process with the same sequence as BulkImportWriter
        _replace_pd_na(df)  # Replace NaN and pd.NA

        # Convert to records format (as done in BulkImportWriter)
        records = df.to_dict(orient="records")

        # Test msgpack serialization and deserialization
        packer = msgpack.Packer()
        for record in records:
            try:
                packed = packer.pack(record)
                unpacked = msgpack.unpackb(packed, raw=False)

                # Verify that special values have been converted to None
                if record.get("float_col") is None:
                    self.assertIsNone(unpacked["float_col"])
                else:
                    self.assertEqual(record["float_col"], unpacked["float_col"])

            except Exception as e:
                self.fail(f"msgpack serialization failed for record {record}: {e}")


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
        self.writer.td_spark.spark.createDataFrame.return_value = MagicMock()
        self.writer.write_dataframe(df, self.table, "overwrite")
        pd.testing.assert_frame_equal(
            self.writer.td_spark.spark.createDataFrame.call_args[0][0], expected_df
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

    def test_write_dataframe_with_boolean_na(self):
        df = pd.DataFrame(
            data=[{"a": True, "b": False}, {"a": False, "b": True, "c": True}],
            dtype="boolean",
        )
        expected_df = pd.DataFrame(
            data=[{"a": "true", "b": "false"}, {"a": "false", "b": "true", "c": "true"}]
        )
        # Convert pd.NA to None in expected_df to match actual processing
        expected_df = expected_df.replace({pd.NA: None})

        self.writer.td_spark.spark.createDataFrame.return_value = MagicMock()
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

    def test_replace_pd_na_with_special_float_values(self):
        """Test that SparkWriter converts special float values to None
        when processing DataFrame"""
        from pytd.writer import _cast_dtypes, _replace_pd_na

        # Create DataFrame with special float values
        df = pd.DataFrame(
            {
                "float_col": [1.0, np.nan, np.inf, -np.inf, 2.5],
                "int_col": [1, 2, 3, 4, 5],
                "str_col": ["a", "b", "c", "d", "e"],
            }
        )

        # Apply processing as SparkWriter does
        _cast_dtypes(df)
        _replace_pd_na(df)

        # With current implementation, NaN becomes None, but inf/-inf remain
        self.assertIsNone(df["float_col"].iloc[1])  # np.nan -> None
        self.assertTrue(np.isinf(df["float_col"].iloc[2]))  # np.inf preserved
        self.assertTrue(np.isinf(df["float_col"].iloc[3]))  # -np.inf preserved
        self.assertEqual(df["float_col"].iloc[0], 1.0)
        self.assertEqual(df["float_col"].iloc[4], 2.5)

    def test_write_dataframe_special_values_consistency(self):
        """Test that SparkWriter handles special values consistently"""
        # Create test DataFrame with special values
        df = pd.DataFrame(
            {
                "float_col": [1.0, np.nan, np.inf, -np.inf],
                "str_col": ["test", None, "value", "data"],
            }
        )

        # Mock the Spark context
        self.writer.td_spark.spark.createDataFrame.return_value = MagicMock()

        # Should not raise any exceptions with special float values
        self.writer.write_dataframe(df, self.table, "overwrite")

        # Verify DataFrame was processed and passed to Spark
        self.assertTrue(self.writer.td_spark.spark.createDataFrame.called)

        # Get the call arguments to createDataFrame
        passed_df = self.writer.td_spark.spark.createDataFrame.call_args[0][0]

        # Verify the processed DataFrame structure
        self.assertEqual(passed_df.shape, (4, 2))
        self.assertListEqual(list(passed_df.columns), ["float_col", "str_col"])

        # Verify special float values are processed according to current implementation
        self.assertIsNone(passed_df["float_col"].iloc[1])  # NaN -> None
        self.assertTrue(np.isinf(passed_df["float_col"].iloc[2]))  # inf preserved
        self.assertTrue(
            np.isinf(passed_df["float_col"].iloc[3])
            and passed_df["float_col"].iloc[3] < 0
        )  # -inf preserved

        # Verify None values are properly handled
        self.assertIsNone(passed_df["str_col"].iloc[1])
