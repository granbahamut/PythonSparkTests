from pyspark.sql import SparkSession
import yaml
import logging.config
from logUtils import LoggerFactory


logger = LoggerFactory.get_logger(__name__, log_level="INFO")


def main():
    logger.warning('Launching an error:')
    try:
        1/0
    except ZeroDivisionError:
        logger.exception("Exception thrown by dividing by zero:")
    finally:
        logger.info("Execution ended.")


if __name__ == '__main__':
    main()

