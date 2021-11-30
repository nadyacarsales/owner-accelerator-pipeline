import logging
import logging.config
from utilities import redshift_config
import redshift_connector
import awswrangler as wr


class RedshiftRepository():
    __connection = None

    def __init__(self, postgres_config: redshift_config.RedshiftConfig, logger=None):
        self.__logger = logger if logger else logging.getLogger(__name__)
        self.__config = postgres_config


    def init_connection(self):
        '''
        Creates connection to Redshift db.
        '''
        if self.__connection is None or self.__connection.closed:
            conn = redshift_connector.connect(
                    host=self.__config.host,
                    database=self.__config.db_name,
                    user=self.__config.username,
                    password=self.__config.password)
            self.__logger.debug(f'RedshiftRepository: created connection to {self.__config.host}, dbname: {self.__config.db_name}')
            self.__connection = conn

        return self.__connection


    def execute_query_fetch_data(self, sql_query, chunksize = None):
        df = None
        try:
            if chunksize is None:
                yield wr.redshift.read_sql_query(sql_query, self.__connection)
            else:
                df_iter = wr.redshift.read_sql_query(sql_query, self.__connection, chunksize=chunksize)
                yield df_iter

        except Exception as ex:
            self.__logger.debug(f'Exception while executing query: {sql_query}.\nException: {str(ex)}')
            raise

        finally:
            self.__close_connection()


    def __close_connection(self):
        try:
            if not self.__connection is None:
                self.__connection.close()
        except Exception as ex:
            self.__logger.debug(f'Exception while trying to close the connection: {str(ex)}')
            

    def __del__(self):
        self.__close_connection()