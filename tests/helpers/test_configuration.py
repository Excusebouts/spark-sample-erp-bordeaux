import unittest
from unittest.mock import patch

from src.helpers.configuration import load_file, load_configuration, init_spark_session

mock_conf = {'name': 'spark-erp-bordeaux-tu'}
mock_schema = {'dataset': 'datasetid'}


class TestConfiguration(unittest.TestCase):

    @patch('builtins.open')
    @patch('yaml.load', return_value=mock_conf)
    @patch('os.path.exists', return_value=True)
    def test__load_file_with_data(self, mock_open, mock_yaml, mock_exist):
        _file = load_file('file.txt')
        self.assertEqual(_file['name'], 'spark-erp-bordeaux-tu')

    @patch('os.path.exists', return_value=False)
    def test__load_file_with_no_file_exist(self, mock_exist):
        with self.assertRaises(FileNotFoundError):
            load_file('file.txt')

    @patch('src.helpers.configuration.load_file', return_value=mock_conf)
    def test_load_configuration(self, mock_load_file):
        _conf = load_configuration()
        mock_load_file.assert_called_once()
        self.assertEqual(_conf['name'], 'spark-erp-bordeaux-tu')

    @patch('src.helpers.configuration.load_file', return_value=mock_schema)
    def test_load_schema(self, mock_load_file):
        _conf = load_configuration()
        mock_load_file.assert_called_once()
        self.assertEqual(_conf['dataset'], 'datasetid')

    def test_init_spark_session_default(self):
        _spark_session = init_spark_session()
        self.assertEqual(_spark_session.conf.get('spark.app.name'), 'spark')
        self.assertEqual(_spark_session.conf.get('spark.master'), 'local')

    def test_init_spark_session(self):
        _spark_session = init_spark_session('local', 'tu')
        self.assertEqual(_spark_session.conf.get('spark.app.name'), 'tu')
        self.assertEqual(_spark_session.conf.get('spark.master'), 'local')


if __name__ == '__main__':
    unittest.main()
