from pyspark.sql.utils import AnalysisException

from src.helpers.configuration import update_spark_log_level, init_spark_session, load_configuration, load_schema
from src.helpers.dataframe import dataframe_flatten
from src.helpers.io import read_from_json, write_to_parquet

_config = load_configuration()
_schema = load_schema()


def run() -> None:
    _spark_session = init_spark_session(app_name=f"[{_config['name']}] Transform JSON to parquet")
    _logger = update_spark_log_level(_config, _spark_session)

    _dataframe_read = None
    try:
        _dataframe_read = read_from_json(_config, _spark_session, _logger)
    except AnalysisException as analysis_exception:
        _logger.error(_config['read']['error'] + analysis_exception.stackTrace)
        exit()

    # https://issues.apache.org/jira/browse/SPARK-18084
    # We can certainly make a dynamic function but I made it simple
    _dataframe_read_flat = dataframe_flatten(_schema, _dataframe_read)

    try:
        write_to_parquet(_config, _dataframe_read_flat, _logger)
    except AnalysisException as analysis_exception:
        _logger.error(_config['write']['error'] + analysis_exception.stackTrace)
        exit()

    _logger.info(f"[{_config['name']}] - End of script")


if __name__ == "__main__":
    run()
