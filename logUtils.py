import yaml
import logging.config


class LoggerFactory(object):
    _LOG = None

    @staticmethod
    def __create_logger(log_file, log_level='INFO', config_path='log_config.yaml'):
        """
        A private method that interacts with the python
        logging module
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
        """
        logger = LoggerFactory.__create_logger(log_file, log_level)
        # return the logger object
        return logger


def load_yaml_config(config_path='log_config.yaml'):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
