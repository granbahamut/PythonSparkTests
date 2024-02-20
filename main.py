from datetime import date
from pyspark.sql import SparkSession
from logUtils import LoggerFactory
import dateUtils


logger = LoggerFactory.get_logger(__name__, log_level="DEBUG")


def main():
    logger.info('Starting program')
    logger.info(dateUtils.get_random_dates_within_rage(date(2024, 1, 1), date(2024, 2, 19), number_of_dates=10))
    logger.info(dateUtils.random_timestamps('2024-01-01 00:00:00', '2024-02-19 23:59:59', number_of_dates=20))
    logger.info('Program terminated successfully.')


if __name__ == '__main__':
    main()

