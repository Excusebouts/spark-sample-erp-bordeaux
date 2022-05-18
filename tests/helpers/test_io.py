import unittest
from unittest.mock import Mock, patch

from src.helpers.configuration import init_spark_session
from src.helpers.io import read_from_json, write_to_parquet

mock_conf = {'name': 'spark-erp-bordeaux-tu', 'read': {'input': 'file_tu'}, 'write': {'mode': 'overwrite', 'partition':
    'field_type', 'output': {'parquet': 'parquet.parquet', 'csv': 'csv.csv'}}}


class TestIO(unittest.TestCase):

    def setUp(self) -> None:
        self.spark_session = init_spark_session()
        self.mock_dataframe_read = self.spark_session.createDataFrame(
            [(1, "foo"), (2, "bar")],
            ["id", "label"]
        )

    @patch('pyspark.sql.readwriter.DataFrameReader.json')
    def test_read_from_json(self, mock_read):
        _logger = Mock()
        mock_read.return_value = self.mock_dataframe_read
        _dataframe_expected = self.spark_session.createDataFrame(
            [(1, "foo"), (2, "bar")],
            ["id", "label"]
        )

        _dataframe_read = read_from_json(mock_conf, self.spark_session, _logger)

        mock_read.assert_called_once()
        _logger.info.assert_called()
        _logger.debug.assert_called_once()
        self.assertTrue(_dataframe_expected.exceptAll(_dataframe_read).count() == 0)

    @patch('pyspark.sql.readwriter.DataFrameWriter.parquet')
    def test_write_to_parquet(self, mock_write):
        _logger = Mock()

        write_to_parquet(mock_conf, self.mock_dataframe_read, _logger)

        mock_write.assert_called_once()
        _logger.info.assert_called()


if __name__ == '__main__':
    unittest.main()
