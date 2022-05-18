from pyspark.sql import SparkSession, DataFrame


def read_from_json(config: dict, spark_session: SparkSession, logger) -> DataFrame:
    logger.info(f"[{config['name']}] Start data read")
    dataframe = spark_session.read.json(config['read']['input'])
    logger.info(f"[{config['name']}] Data read OK")
    logger.debug(dataframe.printSchema())

    return dataframe


def write_to_parquet(config: dict, dataframe: DataFrame, logger) -> None:
    logger.info(f"[{config['name']}] Data write data in parquet")
    dataframe.write.mode(config['write']['mode']) \
        .partitionBy(config['write']['partition']) \
        .parquet(config['write']['output']['parquet'])
    logger.info(f"[{config['name']}] Data write OK")


def write_to_csv(config: dict, dataframe: DataFrame, logger) -> None:
    logger.info(f"[{config['name']}] Data write data in csv")
    dataframe.write.mode(config['write']['mode'])\
        .csv(config['write']['output']['csv'], header=True)
    logger.info(f"[{config['name']} Data write OK")
