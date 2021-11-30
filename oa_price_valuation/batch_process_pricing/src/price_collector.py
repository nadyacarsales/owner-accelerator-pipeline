import time
import traceback
import asyncio
import aiohttp
import logging
import itertools

from pandas.core.frame import DataFrame
from services import livemarket, redbook, instant_offer, price_ahead
from model import price_record
import pandas as pd
import numpy as np

class PriceCollector(object):

    __livemarketService = None
    __instantOfferService = None
    __redbookService = None
    __priceAheadService = None

    PRICING_COLUMNS = ['trade_in_min_price',
                'trade_in_max_price',
                'private_min_price',
                'private_max_price', 
                'lm', 
                'price_ahead_min',
                'price_ahead_max' , 
                'io_min_price' , 
                'io_max_price',
                'redbook_min_price',
                'redbook_max_price',
                'lm_retail_price']
    

    def __init__(self, logger) -> None:
        self.__livemarketService = livemarket.LiveMarketService(logger = logger)
        self.__instantOfferService = instant_offer.InstantOfferService(logger = logger)
        self.__redbookService = redbook.RedbookService(logger = logger)
        self.__priceAheadService = price_ahead.PriceAheadService(logger = logger)
        self.__logger = logger


    async def __collect_price_data_from_apis(self, spot_id_number, rb_code, rb_legacy, kms):
                        #-> List[price_record.TradePrice, price_record.InstantOfferPrice,
                        #                            price_record.RedbookPrice, price_record.PriceAheadPrice]:
        async with aiohttp.ClientSession() as session:
            tasks = []
            tasks.append(self.__livemarketService.get_pricing_data_async(session=session,
                        redbook_legacy_code=rb_legacy, kilometers=kms))
            tasks.append(self.__redbookService.get_pricing_data_async(session=session,
                        spot_id_number=spot_id_number, kilometers=kms))
            tasks.append(self.__instantOfferService.get_pricing_data_async(session=session,
                        spot_id_number=spot_id_number, kilometers=kms))
            tasks.append(self.__priceAheadService.get_pricing_data_async(session=session,
                        rb_code=rb_code, kilometers=kms))
            # asyncio.gather() will wait on the entire task set to be
            # completed.  If you want to process results greedily as they come in,
            # loop over asyncio.as_completed()
            result = await asyncio.gather(*tasks, return_exceptions=True)
        return result
        

    def __get_price_data(self, spot_id, redbook_key, rb_legacy, kms):
        #t_async = []
        
        spot_id_number = spot_id[len('SPOT-ITM-'):]
        #if spot_id_number=='433619':
        #    print('Here')
        try:
            #s = time.perf_counter()
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            done, _ = loop.run_until_complete(
                                    asyncio.wait([self.__collect_price_data_from_apis(spot_id_number, redbook_key, rb_legacy, kms)]))
            loop.close()
            for future in done:
                livemarket_price, redbook_price, io_price, priceahead_price = future.result()
            #elapsed = time.perf_counter() - s
            #t_async.append(elapsed)
            
            price_data = price_record.PriceRecord()
            if priceahead_price:
                price_data.price_ahead_min=priceahead_price.min_price
                price_data.price_ahead_max=priceahead_price.max_price
            if io_price:
                price_data.io_min_price=io_price.min_price
                price_data.io_max_price=io_price.max_price
            price_data.redbook_min_price=redbook_price.private_min_price
            price_data.redbook_max_price=redbook_price.private_max_price

            if livemarket_price and livemarket_price.exists:
                price_data.trade_in_min_price = livemarket_price.min_trade
                price_data.trade_in_max_price = livemarket_price.max_trade
                price_data.private_min_price = livemarket_price.min_price
                price_data.private_max_price = livemarket_price.max_price
                price_data.lm_retail_price = livemarket_price.lm_retail_price
                price_data.lm = 'true'
            else:
                if redbook_price:
                    price_data.trade_in_min_price = redbook_price.trade_in_min_price
                    price_data.trade_in_max_price = redbook_price.trade_in_max_price
                    price_data.private_min_price = redbook_price.private_min_price
                    price_data.private_max_price = redbook_price.private_max_price
                #price_data.lm_retail_price = livemarket_price.lm_retail_price
                price_data.lm = 'false'

            #time.sleep(0.01)
            #print(f'Time to get pricing data: {time.time() - start}')
            return price_data.trade_in_min_price, price_data.trade_in_max_price, \
                    price_data.private_min_price, price_data.private_max_price, \
                    price_data.lm, price_data.price_ahead_min, price_data.price_ahead_max, \
                    price_data.io_min_price, price_data.io_max_price, \
                    price_data.redbook_min_price, price_data.redbook_max_price, price_data.lm_retail_price
        except Exception as ex:
            print(f'Error in price generation, spot_id: {spot_id_number}. Error {ex}')


    def __populate_price_data(self, df:DataFrame):
        #t1 = time.perf_counter()
        

        
        
        df1 = np.vectorize(self.__get_price_data)(df['spot_id'], df['redbook_key'], df['redbookcodelegacy'], df['kms'])
        df[pricing_columns] = list(itertools.zip_longest(*df1, fillvalue=None))
        #t2 = time.perf_counter()
        #print(f'__populate_price_data executed in {t2-t1:.3f}s')
        return df


    def populate_data(self, df):
        start = time.perf_counter()
        try:
            df1 = self.__populate_price_data(df)
            print("Time taken to retrieve pricing data: ", time.perf_counter() - start)
            return df1
        except Exception as error:
            print(error)
            self.__logger.error(error)
        
