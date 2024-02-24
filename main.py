from datetime import date
from pyspark import SparkContext
from pyspark.sql import SparkSession
from utils.logUtils import LoggerFactory
import uuid
import utils.dateUtils as du


logger = LoggerFactory.get_logger(__name__, log_level="DEBUG")
# To avoid Log levels be filled with PySpark nonsense, set this 2 loggers to ERROR:
logger = LoggerFactory.get_logger("pyspark", log_level="ERROR")
logger = LoggerFactory.get_logger("py4j", log_level="ERROR")


def main():
    logger.info('Starting program')
    # logger.debug(du.get_random_dates_within_rage(date(2024, 1, 1), date(2024, 2, 19), number_of_dates=2))
    # logger.debug(du.random_timestamps('2024-01-01 00:00:00', '2024-02-19 23:59:59', number_of_dates=2))
    # logger.debug(str(uuid.uuid4()))
    spark_init()
    logger.info('Program terminated successfully.')


def spark_init():
    file_to_process = 'C:\\Users\\Usuario\\Downloads\\customers-1000000.csv'
    logger.info('Starting spark session...')
    spark = SparkSession.builder.appName("SparkExamples").getOrCreate()
    logger.info('Reading the file {file_to_process}')
    df_pyspark = spark.read.option('header', 'true').csv(file_to_process)
    logger.info(df_pyspark.printSchema())
    df_pyspark.select(['Index', 'First Name']).show()
    logger.info(df_pyspark.dtypes)
    df_pyspark.describe().show()
    

if __name__ == '__main__':
    main()

