from pyspark.sql.utils import AnalysisException

from src.helpers.configuration import init_spark_session, update_spark_log_level, load_configuration, load_schema
from src.helpers.dataframe import get_nb_max_visitors_by_street
from src.helpers.io import read_from_json, write_to_csv

_config = load_configuration()
_schema = load_schema()
logger = None


def run() -> None:
    _spark_session = init_spark_session(app_name=f"[{_config['name']}] Compute visitors by street and accommodation")
    _logger = update_spark_log_level(_config, _spark_session)

    _dataframe_read = None
    try:
        _dataframe_read = read_from_json(_config, _spark_session, _logger)
    except AnalysisException as analysis_exception:
        _logger.error(_config['read']['error'] + analysis_exception.stackTrace)
        exit()

    _nb_visitors_by_street = get_nb_max_visitors_by_street(_schema, _config, _dataframe_read)
    _logger.debug(_nb_visitors_by_street.show())

    try:
        write_to_csv(_config, _nb_visitors_by_street, _logger)
    except AnalysisException as analysis_exception:
        _logger.error(_config['write']['error'] + analysis_exception.stackTrace)
        exit()

    _logger.info(f"[{_config['name']}] - End of script")


if __name__ == "__main__":
    run()
