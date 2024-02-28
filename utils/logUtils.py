import yaml
import logging.config


class LoggerFactory(object):
    _LOG = None

    @staticmethod
    def __create_logger(log_file, log_level='INFO', config_path='log_config.yaml'):
        """
        A private method that interacts with the python
        logging module
        :param log_file: Script name where you want to log.
        :param log_level: Minimal log level to be used
        :param config_path: Path where the logging config is.
        :return: Logger objets to be used on the script declared on the log_file parameter.
        """
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f.read())
        # Initialize the class variable with logger object
        LoggerFactory._LOG = logging.getLogger(log_file)
        logging.config.dictConfig(config)
        # set the logging level based on the user selection
        if log_level == "INFO":
            LoggerFactory._LOG.setLevel(logging.INFO)
        elif log_level == "ERROR":
            LoggerFactory._LOG.setLevel(logging.ERROR)
        elif log_level == "DEBUG":
            LoggerFactory._LOG.setLevel(logging.DEBUG)
        return LoggerFactory._LOG

    @staticmethod
    def get_logger(log_file, log_level):
        """
        A static method called by other modules to initialize logger in
        their own module
        :param log_file: Script name where you want to log.
        :param log_level: Minimal log level to be used
        :return: instance of the logger object.
        """
        logger = LoggerFactory.__create_logger(log_file, log_level)
        # return the logger object
        return logger


def load_yaml_config(config_path='log_config.yaml'):
    """
    Loads the YAML file config
    :param config_path: Logger config file to be used.
    :return: None
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
