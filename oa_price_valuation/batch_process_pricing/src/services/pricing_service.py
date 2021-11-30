import pandas as pd
import logging
import traceback
from services.base_pricing_api_client import BasePricingApiClient

class PricingService():

    PRIMARY_KEY_COLUMN = 'garage_vehicle_id'

    def __init__(self, api_client: BasePricingApiClient, logger: logging.Logger = None) -> None:
        self.__logger = logger if logger else logging.getLogger()
        self.__apiClient = api_client

    def process_data_in_batches(self, df: pd.DataFrame, concurrency=200):
        """
        Gets pricing data from APIs.
        Data processing is done in batches.
        :returns: DataFrame containing pricing data from a particular API
        """
        
        df_final = pd.DataFrame()
        chunk_size=10000
        i=0
        while i<df.shape[0]:
            try:
                res = self.__apiClient.bulk_call_api(df.loc[i:i+chunk_size-1], concurrency=concurrency)
                df_final = pd.concat([df_final, 
                                    pd.DataFrame(res, columns=[self.PRIMARY_KEY_COLUMN]+self.__apiClient.csvColumns())])
                i+=chunk_size
            except Exception as ex:
                print(traceback.print_stack())
        
        return df_final