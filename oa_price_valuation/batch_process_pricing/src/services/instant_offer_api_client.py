from logging import Logger
from services.base_pricing_api_client import BasePricingApiClient
import model.price_record as price_record
import pandas as pd
import model.price_record as price_record


class InstantOfferApiClient(BasePricingApiClient):

    BASE_URL = 'http://redbookinstantofferquote.service.aws.csprd.com.au/v1/quotes/car/amount'
    SPOT_ID_PREFIX = 'SPOT-ITM-'

    def __init__(cls, api_url = None, logger: Logger = None) -> None:
        super().__init__(logger)
        cls.__api_url = api_url if api_url is not None and len(api_url)>0 else cls.BASE_URL


    def csvColumns(self):
        return ['io_min_price', 'io_max_price']

    
    def apiHttpMethod(self) -> str:
        return 'post'


    def baseUrl(self) -> str:
        return self.__api_url


    def prepare_request_data(self, df:pd.DataFrame):
        make_request_string = lambda spot_id, kms: \
                                {"SpotId": spot_id[len(self.SPOT_ID_PREFIX):], "Odometer": (kms), "StateCode": "vic"}
        return list(map(make_request_string, df['spot_id'], df['kms']))


    def parse_response_data(self, garage_vehicle_id, price_response):
        """
        Parses the API response and returns result as tuple of columns, first of which is garage_vehicle_id
        :param: garage_vehicle_id
        :result: tuple of service-specific columns
        """

        price = price_record.InstantOfferPrice()

        try:
            if not price_response:
                return (garage_vehicle_id,)+price.to_list()

            if type(price_response)!=dict:
                self.logger.warn(f'Error parsing InstantOffer API response. garage_vehicle_id: {garage_vehicle_id}. Response: {price_response}.')
                return (garage_vehicle_id,)+price.to_list()

            errors = price_response.get('Errors')
            if errors:
                self.logger.warn(f'Error in InstantOffer API response. garage_vehicle_id: {garage_vehicle_id}. Error {errors[0].get("ErrorMessage")}')
                return (garage_vehicle_id,)+price.to_list()
       
            amount = price_response['Amount']
            price.min_price = amount - (amount * 5 / 100)
            price.max_price = amount + (amount * 5 / 100)
        except Exception as ex:
            self.logger.warn(f'Error parsing InstantOffer API response. garage_vehicle_id: {garage_vehicle_id}. Response: {price_response}. Exception: {ex}')

        return (garage_vehicle_id,) + price.to_list()