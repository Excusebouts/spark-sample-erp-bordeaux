import yaml
import os
from pyspark.sql import SparkSession

CONFIGURATION_FILE = '../resources/configurations/spark-erp-bordeaux.yml'
SCHEMA_FILE = '../resources/configurations/schema.yml'


def load_file(file: str):
    if not os.path.exists(file):
        raise FileNotFoundError(f"File {file} doesn't exist")

    yaml_file = yaml.load(open(file), Loader=yaml.FullLoader)
    return yaml_file


def load_configuration() -> dict:
    return load_file(CONFIGURATION_FILE)


def load_schema() -> dict:
    return load_file(SCHEMA_FILE)


def update_spark_log_level(config: dict, spark_session: SparkSession, log_level: str = 'info'):
    spark_session.sparkContext.setLogLevel(log_level)
    log4j = spark_session._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger(__name__)
    logger.info(f"[{config['name']}] Pyspark script logger initialized")

    return logger


def init_spark_session(master_spark: str = 'local', app_name: str = 'spark') -> SparkSession:
    return SparkSession \
        .builder.master(master_spark) \
        .appName(app_name) \
        .getOrCreate()
