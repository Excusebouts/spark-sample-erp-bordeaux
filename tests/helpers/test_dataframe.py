import unittest

from src.helpers.configuration import init_spark_session, load_file
from src.helpers.dataframe import dataframe_flatten

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


if __name__ == '__main__':
    unittest.main()
