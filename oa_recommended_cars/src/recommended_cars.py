import time
import logging

from pandas.core.frame import DataFrame
from services import recommended_cars_api_client
from funcy import print_durations

class RecommendedCarsCollector(object):

    __apiClient = None
    
    def __init__(self, logger) -> None:
        self.__apiClient = recommended_cars_api_client.RecommendedCarsApiClient(logger = logger)
        self.__logger = logger
        

    def __prepare_data(self, df):
        df['similar_car_array'] = df[['similar_car_1', 'similar_car_2']].agg(','.join, axis=1)


    def __get_recommended_cars(self, df:DataFrame) -> DataFrame:
        df1 = df.copy(deep=True)
        self.__prepare_data(df1)
        rec_car_columns = ['recommended_car_1', 'recommended_car_2', 'recommended_car_3']
        df1[rec_car_columns] = self.__apiClient.bulk_call_api(df1)
        return df1
        

    def populate_data(self, df):
        start = time.perf_counter()
        try:
            df1 = self.__get_recommended_cars(df)
            print("Time taken to retrieve recommended cars: ", time.perf_counter() - start)
            return df1
        except Exception as error:
            print(error)
            self.__logger.error(error)


    
        
