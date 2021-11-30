from logging import Logger
import logging
import time
import pandas as pd
from services.base_pricing_api_client import BasePricingApiClient
import model.price_record as price_record

class RedbookApiClient(BasePricingApiClient):

    BASE_URL = 'http://autocalcv2.prod.rbdata.csnglobal.net/v2/'
    SPOT_ID_PREFIX = 'SPOT-ITM-'

    def __init__(self, api_url = None, logger: Logger = None) -> None:
        super().__init__(logger)
        self.__api_url = api_url if api_url is not None and len(api_url)>0 else self.BASE_URL


    def csvColumns(self):
        return ['rb_trade_in_min_price', 'rb_trade_in_max_price', 'rb_private_min_price', 'rb_private_max_price']


    def apiHttpMethod(self) -> str:
        return 'get'


    def baseUrl(self) -> str:
        return self.__api_url

    def prepare_request_data(self, df:pd.DataFrame):
        make_request_string = lambda spot_id, kilometers: \
                f'{self.__api_url}{spot_id[len(self.SPOT_ID_PREFIX):]}?kms={kilometers}'
        return list(map(make_request_string, df['spot_id'], df['kms']))

    
    def parse_response_data(self, garage_vehicle_id, price_response):
        """
        Parses the API response and returns result as tuple of columns, first of which is garage_vehicle_id
        :param: garage_vehicle_id
        :result: tuple of service-specific columns
        """
        price = price_record.RedbookPrice()
        try:
            if not price_response:
                return (garage_vehicle_id,)+price.to_list()

            if type(price_response)!=dict:
                self.logger.warn(f'Error parsing Redbook API response. garage_vehicle_id: {garage_vehicle_id}. Response: {price_response}.')
                return (garage_vehicle_id,)+price.to_list()

            item = price_response['item']
            
            price.trade_in_min_price = item['tradeInMinPrice']
            price.trade_in_max_price = item['tradeInMaxPrice']
            price.private_min_price = item['privateMinPrice']
            price.private_max_price = item['privateMaxPrice']
        
        except Exception as ex:
            self.logger.warn(f'Error parsing Redbook API response. garage_vehicle_id: {garage_vehicle_id}. Response: {price_response}. Exception: {ex}')

        return (garage_vehicle_id,) + price.to_list()
        