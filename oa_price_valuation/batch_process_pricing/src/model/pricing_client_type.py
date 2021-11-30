from enum import Enum

class PricingClientType(Enum):
    LiveMarket = 'livemarket'
    Redbook = 'redbook'
    InstantOffer = 'instantoffer'
    PriceAhead = 'priceahead'