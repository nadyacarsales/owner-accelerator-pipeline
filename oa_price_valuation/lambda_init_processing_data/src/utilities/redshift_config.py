from utilities import settings_util
import json

class RedshiftConfig():

    _params = None

    def __init__(self, db_params: settings_util.Settings, params_prefix = None, logger = None):
        self._db_params = db_params
        self._params_prefix = params_prefix
        self._logger = logger

    def _get_param(self, name):
        if self._params is None:
            self._params = json.loads(self._db_params.get_parameter_by_name(f'/{self._params_prefix}'))
        return self._params[name]

    @property
    def host(self):
        return self._get_param('host')

    @property
    def db_name(self):
        return self._get_param('dbname')

    @property
    def username(self):
        return self._get_param('user')

    @property
    def password(self):
        return self._get_param('password')

    @property
    def port(self):
        return '5439'
