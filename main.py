from datetime import date
from pyspark import SparkContext
from pyspark.errors import PySparkTypeError
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import monotonically_increasing_id, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

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
    file_to_process = 'C:\\Users\\Usuario\\Downloads\\aso1998.txt'
    logger.info('Starting spark session...')
    spark = SparkSession.builder.appName("SparkExamples").getOrCreate()
    logger.info('Reading the file {file_to_process}')
    try:
        # Temp structure to index all the data
        temp_schema = StructType([
            StructField('value', StringType(), nullable=True),
            StructField('index', LongType(), nullable=False)
        ])
        # Reading the file and adding an index to find header and totals
        df_detail = spark.read.text(file_to_process)
        df_detail = df_detail.withColumn('index', monotonically_increasing_id())
        df_header = spark.createDataFrame(data=[df_detail.first()], schema=temp_schema)
        df_totals = spark.createDataFrame(data=[df_detail.tail(1)[0]], schema=temp_schema)

        df_header.show()
        # df_detail.join(df_totals, df_totals.index != df_detail.index, 'right')\
        #    .join(df_header, df_header.index != df_detail.index, 'right')\
        #    .select(df_detail.index, df_detail.value).show()
        # TODO: Correct this filter
        # df_detail.filter(~col(df_detail.index).isin([df_header[0]['index'], df_totals[0]['index']])).show()
        df_totals.show()

        file_header = [('registerType', 2, 'NUM', '01'),
                       ('mainCompanyCode', 13, 'NUM', None),
                       ('firstExpiryDate', 8, 'NUM', 'yyyyMMdd'),
                       ('secondExpiryDate', 8, 'NUM', 'yyyyMMdd'),
                       ('billingDate', 8, 'NUM', 'yyyyMMdd'),
                       ('cycle', 3, 'ALN', None),
                       ('filler', 'ANY', ' ')]

        file_detail = [('registerType', 2, 'NUM', '02'),
                       ('identificationNumber', 25, 'NUM', None),
                       ('billID', 12, 'NUM', None),
                       ('billedPeriod', 2, 'NUM', None),
                       ('mainServiceAmount', 13, 'NUM', None),
                       ('additionalCompanyID', 13, 'NUM', None),
                       ('additionalServiceAmount', 13, 'NUM', None),
                       ('filler', 4, 'ANY', ' ')]

        file_totals = [('registerType', 2, 'NUM', '03'),
                       ('registerCount', 9, 'NUM', None),
                       ('mainServiceTotal', 18, 'NUM', None),
                       ('additionalServiceTotal', 18, 'NUM', None),
                       ('filler', 37, 'NUM', None)]

        df_detail.withColumn(file_header[0][0], df_detail['value'].substr(1, 2)) \
            .withColumn(file_header[1][0], df_detail['value'].substr(3, 13)) \
            .withColumn(file_header[2][0], df_detail['value'].substr(16, 8)) \
            .withColumn(file_header[3][0], df_detail['value'].substr(24, 8)) \
            .withColumn(file_header[4][0], df_detail['value'].substr(32, 8)) \
            .withColumn(file_header[5][0], df_detail['value'].substr(40, 3)) \
            .withColumn(file_header[6][0], df_detail['value'].substr(43, 42)) \
            .drop('value').show()
        # df_detail.filter(((df_detail['Data_value'] <= 2463) | (df_detail['Data_value'] >= 8000)) &
        #                   (df_detail['Period'] == '2013.03')).select(['Period', 'Data_value', 'Group']).show()
    except AnalysisException as e:
        logger.error(f'Error reading the file_header assigned: {e}')
    except PySparkTypeError as e:
        logger.error(f'Error with types: {e}')
        raise PySparkTypeError(e)


if __name__ == '__main__':
    main()
