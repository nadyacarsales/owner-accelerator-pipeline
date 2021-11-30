from abc import ABCMeta, abstractmethod
import json
from logging import Logger
import time
import logging
import traceback
from typing import List
import asyncio
from aiohttp import ClientSession
from aiohttp.connector import TCPConnector
from aiolimiter import AsyncLimiter
import pandas as pd

class BasePricingApiClient(metaclass=ABCMeta):

    @property
    @abstractmethod
    def csvColumns(self):
        raise NotImplementedError

    @abstractmethod
    def prepare_request_data(self, df:pd.DataFrame):
        return NotImplementedError

    @abstractmethod
    def parse_response_data(self, garage_vehicle_id, result):
        return NotImplementedError

    def __init__(cls, logger: Logger = None) -> None:
        cls._logger = logger if logger else logging.getLogger()

    @property
    def logger(self):
        return self._logger

    @property
    @abstractmethod
    def apiHttpMethod(self) -> str:
        return 'get'

    @property
    @abstractmethod
    def baseUrl(self) -> str:
        raise NotImplementedError

    async def __post_request(cls, session:ClientSession, url, method='get', data='', **kwargs):
        '''
        Posts request to the specified url.
        :param: url: Url to send the requests to.
        :param: method: http method to use. Curently supported: get, post
        :param: data: payload to be sent to url. If method=get, it's the url with parameters. If method=post, it's the json payload.
        '''
        try:
            #print(f'Sending request... {datetime.datetime.now().strftime("%X")}')
            price_response = None
            if method=='get':
                async with session.get(url, **kwargs) as response:
                    if response.status == 200:
                        try:
                            if response.content_type.lower() == 'application/json':
                                price_response = await response.json()
                            else:
                                price_response = await response.text()
                        except ValueError:
                            pass
                    else:
                        price_response = await response.text()
                        cls.logger.debug(f'Error in post_request: {price_response}. RequestUrl: {url}. Data: {data}')
                    return price_response
            else:
                async with session.post(url, data=data, **kwargs) as response:
                    if response.status == 200:
                        try:
                            if response.content_type.lower() == 'application/json':
                                price_response = await response.json()
                            else:
                                price_response = await response.text()
                        except ValueError:
                            pass
                    else:
                        price_response = await response.text()
                        cls.logger.debug(f'Error in post_request: {price_response}. RequestUrl: {url}. Data: {data}')
                    return price_response
        except TimeoutError:
            raise
        except Exception as ex:
            print(f'Exception getting recommended cars for url: {url}. Data: {data}: {ex}')
            return None

    @classmethod
    async def __gather_with_concurrency(cls, concurrency, *tasks, return_exceptions=False):
        limiter = AsyncLimiter(concurrency, 1)

        async def bounded_task(task):
            async with limiter:
                return await task

        return await asyncio.gather(*(bounded_task(task) for task in tasks), return_exceptions=return_exceptions)

    
    async def __bulk_call_api_internal(self, request_data_prepared, concurrency:int = 100):
        '''
        Creates list of tasks (API requests) with required request rate and concurrency.
        :param: request_data_prepared: requests to the sent (if GET, then it's url with parameters. If POST, it's the base url)
        :param: concurrency: request rate per second
        '''
        TIMEOUT = 1000
        headers = {'Content-Type': 'application/json'}

        try:
            async with ClientSession(connector=TCPConnector(limit_per_host=concurrency)) as session:
                if self.apiHttpMethod().lower()=='get':
                    reqs = [asyncio.wait_for( self.__post_request(session, data, headers=headers), 
                                                    timeout=TIMEOUT) for data in request_data_prepared]
                else:
                    reqs = [asyncio.wait_for( self.__post_request(session, self.baseUrl(), method='post', data=json.dumps(data), 
                                                    headers=headers), timeout=TIMEOUT) for data in request_data_prepared]

                responses = await self.__gather_with_concurrency(concurrency, *reqs, return_exceptions=False)
            return responses
        except Exception as ex:
            print(f'Exception in __bulk_call_api_internal: {str(ex)}')


    def bulk_call_api(self, request_data:pd.DataFrame, primary_key:str='garage_vehicle_id', concurrency:int=100) -> List:
        '''
        Calls the API and returns API response
        :param: request_data: DataFrame containing all columns (including primary key) required for the request
        :param: primary_key: table's unique key which can me used to join back API results with the original data
        :returns: List of values from API response in form [garage_vehicle_id, resp_field_1, ... resp_field_n]
        '''

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            request_data_prepared = self.prepare_request_data(request_data)

            t1 = time.perf_counter()
            results = loop.run_until_complete(self.__bulk_call_api_internal(request_data_prepared, concurrency))
            # Zero-sleep to allow underlying connections to close. See https://docs.aiohttp.org/en/stable/client_advanced.html for details
            loop.run_until_complete(asyncio.sleep(0))
            loop.close()
            
            #print(f'Results: {results}')
            print(f'Time for request data from API: {time.perf_counter()-t1:.3f}s')

            processed_result = list()
            for ind, res in enumerate(results):
                processed_result.append(self.parse_response_data(request_data.loc[request_data.index[ind], primary_key], res))
            
            return processed_result
        except Exception as ex:
            print(f'Exception in bulk_call_api: {ex}. \nTrace: {traceback.print_exc()}')
            raise ex