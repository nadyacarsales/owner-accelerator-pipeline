from logging import Logger
from services.base_pricing_api_client import BasePricingApiClient
import model.price_record as price_record
import pandas as pd
from datetime import date


class DateRange:
    def __init__(self):
        self.today = date.today()
        self.start = self.p_1()
        self.end = self.p_2()

    def p_1(self):
        return date(self.today.year + 1, self.today.month, self.today.day).strftime('%Y-%m')

    def p_2(self):
        return date(self.today.year + 2, self.today.month, self.today.day).strftime('%Y-%m')


class PriceAheadApiClient(BasePricingApiClient):

    BASE_URL = 'http://did-priceahead-api.prod.rbdata.csnglobal.net/v1/priceahead/'

    def __init__(cls, api_url = None, logger: Logger = None) -> None:
        super().__init__(logger)
        cls.__api_url = api_url if api_url is not None and len(api_url)>0 else cls.BASE_URL


    def csvColumns(self):
        return ['price_ahead_min_price', 'price_ahead_max_price']


    def apiHttpMethod(self) -> str:
        return 'get'


    def baseUrl(self) -> str:
        return self.__api_url


    def prepare_request_data(self, df:pd.DataFrame):
        make_request_string = lambda redbook_code, kms: \
                                 self.__format_url(redbook_code, DateRange(), kms+10000, kms+20000)
        return list(map(make_request_string, df['redbook_key'], df['kms']))


    def parse_response_data(self, garage_vehicle_id, price_response):
        """
        Parses the API response and returns result as tuple of columns, first of which is garage_vehicle_id
        :param: garage_vehicle_id
        :result: tuple of service-specific columns
        """

        pa_price = price_record.PriceAheadPrice()
        try:
            if not price_response:
                return (garage_vehicle_id,)+pa_price.to_list()
       
            if type(price_response)!=list:
                self.logger.warn(f'Error parsing PriceAhead API response. garage_vehicle_id: {garage_vehicle_id}. Response: {price_response}.')
                return (garage_vehicle_id,)+pa_price.to_list()
            
            #assert len(price_response)==2
            
            if 'errorMessage' not in price_response[0]:
                pa_price.min_price = price_response[0]['good']
            if 'errorMessage' not in price_response[1]:
                pa_price.max_price = price_response[1]['good']
            
        except Exception as ex:
            self.logger.warn(f'Error parsing PriceAhead API response. garage_vehicle_id: {garage_vehicle_id}. Response: {price_response}. Exception: {ex}')

        return (garage_vehicle_id,) + pa_price.to_list()


    def __format_url(self, rb_code, date_range, kms1, kms2):
        return f'{self.__api_url}' + \
            f'projectKmPrice?rbcode15={rb_code}' + \
            f'&PriceType=Wholesale&KmsPIT[0].PointInTime={date_range.start}' + \
            f'&KmsPIT[0].Kms={kms1}&KmsPIT[1].PointInTime={date_range.end}' + \
            f'&KmsPIT[1].Kms={kms2}'

