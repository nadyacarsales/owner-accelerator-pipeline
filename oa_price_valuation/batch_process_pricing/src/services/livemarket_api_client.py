from logging import Logger
from services.base_pricing_api_client import BasePricingApiClient
import model.price_record as price_record
import pandas as pd

class LiveMarketApiClient(BasePricingApiClient):

    BASE_URL = 'http://app.livemarket.aws.csprd.com.au/integration/internal/priceguide/'

    
    def __init__(self, api_url = None, logger: Logger = None) -> None:
        super().__init__(logger)
        self.__api_url = api_url if api_url is not None and len(api_url)>0 else self.BASE_URL


    def csvColumns(self):
        return ['lm_retail_price', 'lm_min_trade', 'lm_max_trade', 'lm_min_price', 'lm_max_price', 'lm_exists']


    def apiHttpMethod(self) -> str:
        return 'get'


    def baseUrl(self) -> str:
        return self.__api_url


    def prepare_request_data(self, df:pd.DataFrame):
        make_request_string = lambda redbook_legacy_code, kilometers: \
                f'{self.__api_url}{redbook_legacy_code}?getConfidence=1&kms={kilometers}'
        return list(map(make_request_string, df['redbookcodelegacy'], df['kms']))


    def parse_response_data(self, garage_vehicle_id, price_response):
        """
        Parses the API response and returns result as tuple of columns, first of which is garage_vehicle_id
        :param: garage_vehicle_id
        :result: tuple of service-specific columns
        """
        trade_price = price_record.TradePrice()
        try:
            if not price_response:
                return (garage_vehicle_id,)+trade_price.to_list()

            if type(price_response)!=dict:
                self.logger.warn(f'Error parsing LiveMarket API response. garage_vehicle_id: {garage_vehicle_id}. Response: {price_response}.')
                return (garage_vehicle_id,)+trade_price.to_list()

            trade = price_response['wholesale']
            retail_gc = price_response['retail-egc']
            trade_price.exists = bool(trade) and bool(retail_gc)
            
            if trade:
                trade_price.min_trade = trade - (trade * 5 / 100)
                trade_price.max_trade = trade + (trade * 5 / 100)

            if retail_gc:
                trade_price.lm_retail_price = retail_gc
                trade_price.min_price = retail_gc - (retail_gc * 5 / 100)
                trade_price.max_price = retail_gc + (retail_gc * 5 / 100)
            
        except Exception as ex:
            self.logger.warn(f'Error parsing LiveMarket API response. garage_vehicle_id: {garage_vehicle_id}. Response: {price_response}. Exception: {ex}')
            
        return (garage_vehicle_id,) + trade_price.to_list()
  