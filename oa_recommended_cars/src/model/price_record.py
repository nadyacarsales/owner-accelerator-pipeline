class PriceRecord(object):

    def __init__(self):
        self.trade_in_min_price=0
        self.trade_in_max_price=0
        self.private_min_price=0
        self.private_max_price=0
        self.lm=''
        self.price_ahead_min=0
        self.price_ahead_max=0
        self.io_min_price=0
        self.io_max_price=0
        self.redbook_min_price=0
        self.redbook_max_price=0
        self.lm_retail_price=0


class TradePrice(object):

    def __init__(self):
        self.lm_retail_price = None
        self.min_trade = None
        self.max_trade = None
        self.min_price = None
        self.max_price = None
        self.exists = False


class InstantOfferPrice(object):

    def __init__(self):
        self.min_price = None
        self.max_price = None


class RedbookPrice(object):

    def __init__(self):
        self.trade_in_min_price = None
        self.trade_in_max_price = None
        self.private_min_price = None
        self.private_max_price = None


class PriceAheadPrice(object):

    def __init__(self):
        self.min_price = None
        self.max_price = None