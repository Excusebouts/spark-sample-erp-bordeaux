from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, lower, col


def dataframe_flatten(schema: dict, dataframe_read: DataFrame) -> DataFrame:
    _dataframe_read_fields = dataframe_read \
        .select(schema['record']['id'], schema['fields']['all']) \
        .withColumnRenamed(schema['fields']['type'], schema['renamed']['field'])

    return dataframe_read \
        .select(schema['dataset'], schema['record']['id'], schema['geometry'], schema['record']['timestamp']) \
        .join(_dataframe_read_fields, on=schema['record']['id'], how='inner') \
        .withColumnRenamed(schema['fields']['type'], schema['renamed']['geometry'])


def _standardization_of_streets_name(schema: dict, config: dict, dataframe_read: DataFrame) -> DataFrame:
    return dataframe_read \
        .withColumn(schema['street'], regexp_replace(schema['address'], config['regexp']['street'], '')) \
        .withColumn(schema['street'], lower(col('street')))


def get_nb_max_visitors_by_street(schema: dict, config: dict, dataframe_read: DataFrame) -> DataFrame:
    _dataframe_street_standardized = _standardization_of_streets_name(schema, config, dataframe_read)

    return _dataframe_street_standardized\
        .select(schema['street'], schema['fields']['accommodation'], schema['fields']['visitors']) \
        .groupby(schema['street'], schema['accommodation']) \
        .sum(schema['visitors'])
