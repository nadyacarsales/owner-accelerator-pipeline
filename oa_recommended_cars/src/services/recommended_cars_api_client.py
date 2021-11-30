import logging
import traceback
from typing import List
import asyncio
import aiohttp
from aiohttp import ClientSession, TCPConnector
import json
import pandas as pd
import numpy as np
import time
import datetime
from aiolimiter import AsyncLimiter
from itertools import chain, repeat, islice
from test_async import gather_with_concurrency

class RecommendedCarsApiClient(object):

    BASE_URL = 'http://api-retail-recommendations.aws.csprd.com.au/v1/recommendations/carsales'

    def __init__(cls, api_url = None, logger: logging.Logger = None) -> None:
        super().__init__()
        cls.__api_url = api_url if api_url is not None and len(api_url)>0 else cls.BASE_URL
        cls.__logger = logger if logger else logging.getLogger()


    async def __post_request(self, session, url, data, **kwargs):
        try:
            #print(f'Sending request... {datetime.datetime.now().strftime("%X")}')
            async with session.post(url, data=data, **kwargs) as response:
                response = await response.text()
                #await asyncio.sleep(0.01)
                return response
        except TimeoutError:
            raise
        except Exception as ex:
            print(f'Exception getting recommended cars for {data}: {ex}')
            return None


    def __prepare_request_data(self, df:pd.DataFrame):
        make_json = lambda member_id, similar_car_array: {
                "userId": str(member_id),
                "placements": [
                    {
                        "name": "owner-accelerate",
                        "filters": {
                            "item-ids": similar_car_array
                        },
                        "maxItems": 3
                    }
                ]
            }
        return np.vectorize(make_json)(df['customer_id'], df['similar_car_array'])        


    def __process_result(self, customer_id, result):
        try:
            price_response = json.loads(result)[0]
            if bool(price_response['isSucceed']):
                return list( islice(chain(map(lambda item: item['id'], price_response['items']), repeat(None)), 3) )
            else:
                self.__logger.warn(f'Failed to get price recommendation response from API for customer_id={customer_id}. Response: {result}')
                return [None, None, None]
        except Exception as ex:
            self.__logger.warn(f'Error parsing API response. Customer_id: {customer_id}. Response: {result}. Exception: {ex}')
            return [None, None, None]

    async def gather_with_concurrency(concurrency, *tasks, return_exceptions=False):
        #semaphore = asyncio.Semaphore(concurrency)
        limiter = AsyncLimiter(concurrency, 1)

        async def sem_task(task):
            async with limiter:
                return await task

        return await asyncio.gather(*(sem_task(task) for task in tasks), return_exceptions=return_exceptions)


    async def __bulk_call_api_internal(self, request_data_prepared):
        CONCURRENCY = 120
        TIMEOUT = 1000
        headers = {'Content-Type': 'application/json'}

        try:
            async with ClientSession(connector=TCPConnector(limit_per_host=CONCURRENCY)) as session:
                reqs = [asyncio.wait_for( self.__post_request(session, self.__api_url, data=json.dumps(data), 
                                                headers=headers), timeout=TIMEOUT) for data in request_data_prepared]

                responses = await gather_with_concurrency(CONCURRENCY, *reqs, return_exceptions=False)
            return responses
        except Exception as ex:
            print(f'Exception in __bulk_call_api_internal: {str(ex)}')


    def bulk_call_api(self, request_data:pd.DataFrame) -> List:

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            request_data_prepared = self.__prepare_request_data(request_data)

            t1 = time.perf_counter()
            results = loop.run_until_complete(self.__bulk_call_api_internal(request_data_prepared))
            
            # Zero-sleep to allow underlying connections to close. See https://docs.aiohttp.org/en/stable/client_advanced.html for details
            loop.run_until_complete(asyncio.sleep(0))
            loop.close()
            
            print(f'Results: {results}')
            print(f'Time for request data: {time.perf_counter()-t1:.3f}s')

            processed_result = list()
            for ind, res in enumerate(results):
                processed_result.append(self.__process_result(request_data.loc[request_data.index[ind], 'customer_id'], res))
            
            return processed_result
        except Exception as ex:
            print(f'Exception in bulk_call_api: {ex}. \nTrace: {traceback.print_exc()}')
            raise ex


        
