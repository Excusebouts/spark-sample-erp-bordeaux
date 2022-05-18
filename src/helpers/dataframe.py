from pyspark.sql import DataFrame


def dataframe_flatten(schema: dict, dataframe_read: DataFrame) -> DataFrame:
    _dataframe_read_fields = dataframe_read \
        .select(schema['record']['id'], schema['fields']['all']) \
        .withColumnRenamed(schema['fields']['type'], schema['renamed']['field'])

    return dataframe_read \
        .select(schema['dataset'], schema['record']['id'], schema['geometry'], schema['record']['timestamp']) \
        .join(_dataframe_read_fields, on=schema['record']['id'], how='inner') \
        .withColumnRenamed(schema['fields']['type'], schema['renamed']['geometry'])
