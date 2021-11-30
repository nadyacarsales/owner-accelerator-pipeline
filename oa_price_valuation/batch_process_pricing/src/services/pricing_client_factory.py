from model.pricing_client_type import PricingClientType
from services import livemarket_api_client, redbook_api_client, instant_offer_api_client, price_ahead_api_client

class PricingClientFactory:
    '''
    Factory class that creates API clients for pricing APIs.
    '''

    __livemarketClient = None
    __redbookClient = None
    __instantOfferClient = None
    __priceAheadClient = None

    def __init__(self, logger) -> None:
        self.__livemarketClient = livemarket_api_client.LiveMarketApiClient(logger = logger)
        self.__redbookClient = redbook_api_client.RedbookApiClient(logger = logger)
        self.__instantOfferClient = instant_offer_api_client.InstantOfferApiClient(logger = logger)
        self.__priceAheadClient = price_ahead_api_client.PriceAheadApiClient(logger = logger)


    def get_client(self, client_type: PricingClientType):
        clients = {
            PricingClientType.LiveMarket: self.__livemarketClient,
            PricingClientType.Redbook: self.__redbookClient,
            PricingClientType.InstantOffer: self.__instantOfferClient,
            PricingClientType.PriceAhead: self.__priceAheadClient
        }
        client = clients.get(client_type)
        return client if client else ValueError(f'Pricing client type {client_type} is not supported.')