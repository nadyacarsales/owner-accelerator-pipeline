from pandas.core.frame import DataFrame
import logging
import logging.config
import sys
import os
from pathlib import Path
from datetime import datetime
from utilities.redshift_config import RedshiftConfig, settings_util
from repository import redshift_repository
from recommended_cars import RecommendedCarsCollector
import pandas as pd
from utilities import s3_util
from funcy import print_durations

pd.options.mode.chained_assignment = None


SECRET_KEYS_PREFIX = 'redshift-cspot'
#DEFAULT_LOGGING_FILENAME = 'logging_recomm_cars.conf'

S3_OUTPUT_BUCKET = os.environ.get('S3_OUTPUT_BUCKET', 'dcp-prod-owner-accelerator')
S3_OUTPUT_PATH_KEY = os.environ.get('S3_OUTPUT_PATH_KEY', 'recommended_cars')
S3_OUTPUT_FILE_NAME = 'recommended-cars-data.csv'

RECOMM_CARS_QUERY = '''
    select customer_id_ as customer_id, garage_guid, similar_car_1, similar_car_2, getdate() as load_dt  
    from cspot_owner_acc.cspot_OA_combined 
    where similar_car_1 is not null and similar_car_2 is not null
'''

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@print_durations()
def fetch_data() -> DataFrame:
    """
    Retrieves data from database.
    """
    cspot_config = RedshiftConfig(settings_util.ParameterStoreSettings(logger), 
                                                    params_prefix=SECRET_KEYS_PREFIX)
    repo = redshift_repository.RedshiftRepository(cspot_config, logger)
    repo.init_connection()
    
    df = None
    for df_iter in repo.execute_query_fetch_data(RECOMM_CARS_QUERY, chunksize=10000):
        for d in df_iter:
            df = pd.concat([df,d], ignore_index=True)
    
    return df
    

@print_durations()
def process_data_in_batches(df: DataFrame):
    """
    Gets recommended cars and saves them to S3.
    Data processing is done in threads.
    """
    rec_cars_collector = RecommendedCarsCollector(logger)
    s3 = s3_util.S3Util(logger=logger)
    s3_path = f's3://{S3_OUTPUT_BUCKET}/{S3_OUTPUT_PATH_KEY}/{S3_OUTPUT_FILE_NAME}'
    logger.info(f'Output S3 path: {s3_path}')

    df_final = DataFrame()
    chunk_size=10000
    i=0
    while i<df.shape[0]:
        res = rec_cars_collector.populate_data(df.loc[i:i+chunk_size-1])
        df_final = pd.concat([df_final, res])
        i+=chunk_size
    
    #res = rec_cars_collector.populate_data(df)
    s3.write_csv(df_final[['garage_guid', 'recommended_car_1', 'recommended_car_2', 'recommended_car_3', 'load_dt']], s3_path)
    

############################################################
@print_durations()
def main():

    # get data from db
    df = fetch_data()
    #df = wr.s3.read_csv(path=f's3://nadya-dev-insights/oa/src_data/recommended_cars.csv')
    logger.info(f'Retrieved data from db. Rows: {df.shape[0]}')

    if df.shape[0] > 0:
        # process data
        process_data_in_batches(df)

    logger.info('All done.')

if __name__=='__main__':
    main()
