import unittest

from src.helpers.configuration import init_spark_session
from src.helpers.dataframe import dataframe_flatten, get_nb_max_visitors_by_street

mock_conf = {'regexp': {'street': '[^a-zA-Z]+'}}
mock_schema = {'dataset': 'datasetid', 'record': {'id': 'recordid', 'timestamp': 'record_timestamp'},
               'address': 'fields.adresse_1', 'fields': {'all': 'fields.*', 'accommodation': 'fields.avec_hebergement',
                                                         'visitors': 'fields.nb_visiteurs_max', 'type': 'type'},
               'geometry': 'geometry.*', 'renamed': {'field': 'field_type', 'geometry': 'geometry_type'},
               'accommodation': 'avec_hebergement', 'visitors': 'nb_visiteurs_max', 'street': 'street'}


class TestDataframe(unittest.TestCase):

    def setUp(self) -> None:
        self.spark_session = init_spark_session()
        self.dataframe_test = self.spark_session.read \
            .option("multiline", "true") \
            .json('../../resources/test/data_test.json')

    def test_dataframe_flatten(self):
        _df_flatten = dataframe_flatten(mock_schema, self.dataframe_test)
        self.assertTrue(_df_flatten.schema.simpleString().find("field_type"))
        self.assertTrue(_df_flatten.schema.simpleString().find("geometry_type"))

    def test_get_nb_max_visitors_by_street(self):
        _columns_expected = ["street", "avec_hebergement", "sum(nb_visiteurs_max)"]
        _data_expected = [("courslintendance", "1", 0), ("courslintendance", "0", 25)]
        _rdd = self.spark_session.sparkContext.parallelize(_data_expected)
        _dataframe_expected = _rdd.toDF(_columns_expected)

        _nb_visitors_by_street = get_nb_max_visitors_by_street(mock_schema, mock_conf, self.dataframe_test)

        self.assertTrue(_dataframe_expected.exceptAll(_nb_visitors_by_street).count() == 0)


if __name__ == '__main__':
    unittest.main()
