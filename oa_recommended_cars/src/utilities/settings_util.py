import boto3
import logging

class Settings(object):
    """
    Base Config class
    """

    _logger = None

    def __init__(self, logger=None):
        """
        Arguments:
            logger -- (optional)

        Returns:
            None
        """
        self._logger = logging.getLogger(self.__class__.__name__) if logger is None else logger
        

    def get_parameter_by_name(self, parameter_name):
        return ''


class ParameterStoreSettings(Settings):
    """
    Reads settings from AWS Parameter Store
    """

    ssm = boto3.client('ssm', region_name='ap-southeast-2')

    def __init__(self, logger=None):
        """
        Arguments:
            logger -- (optional)

        Returns:
            None

        """
        self._logger = logging.getLogger(self.__class__.__name__) if logger is None else logger


    def get_parameter_by_name(self, parameter_name):
        """
        Reads parameter from Parameter Store
        Arguments:
            parameter name -- full parameter path 

        Returns:
            parameter value
        """
        parameter = self.ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        return parameter['Parameter']['Value']