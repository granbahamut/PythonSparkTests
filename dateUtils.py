import datetime
import random
from random import choices
from datetime import timedelta
from logUtils import LoggerFactory

logger = LoggerFactory.get_logger(__name__, log_level="INFO")


def random_timestamps(start_time, end_time, number_of_dates=1, date_format='%Y-%m-%d %H:%M:%S'):
    """
    This function returns a list of timestamps between a start date and end date, the number of times given by the
    number_of_dates attribute. the given format for the start dates must be in the formate given on the date_format
    format.
    :param start_time: Starting date of the range.
    :param end_time: End date of the range.
    :param number_of_dates: Random dates to be extracted from the range, using the range function.
    :param date_format: Default: %Y-%m-%d %H:%M:%S, but can be changed to any format given.
    :return: A list of dateTime objects.
    """
    try:
        start_time = datetime.datetime.strptime(start_time, date_format)
        end_time = datetime.datetime.strptime(end_time, date_format)
        td = end_time - start_time
        return [random.random() * td + start_time for _ in range(number_of_dates)]
    except ValueError as e:
        error_message = f'Invalid format for start_date and/or end_date, default format: ' \
                        f'date_format=%Y-%m-%d %H:%M:%S, found this format: {date_format} in case you\'ve changed it.'
        logger.error(error_message)
        raise ValueError(error_message) from e


def get_random_dates_within_rage(date_start, date_end, number_of_dates=1):
    """
    This function returns a list of dates between a start date and end date, the number of times given by the
    number_of_dates attribute.
    :param date_start: Starting date of the range.
    :param date_end: End date of the range.
    :param number_of_dates: Random dates to be extracted from the range, using random.choices(n, k) function.
    :return: A list of random dates within a range.
    :except: OverflowError For when star_date is greater than end_date or number_of_dates is less than 1.
    """
    logger.info('dates given are: {} and {}'.format(date_start, date_end))
    if number_of_dates < 1:
        number_of_dates = 1
    resulting_dates = [date_start]

    try:
        while date_start != date_end:
            date_start += timedelta(days=1)
            resulting_dates.append(date_start)
    except OverflowError as oe:
        error_message = 'Start date must be lower than end date.'
        logger.error(error_message)
        raise OverflowError(error_message) from oe

    return choices(resulting_dates, k=number_of_dates)
