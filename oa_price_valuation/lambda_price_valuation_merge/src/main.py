import time
import datetime
import logging
import os
import numpy as np
from pandas.core.frame import DataFrame
import awswrangler as wr
import pandas as pd

S3_BUCKET = os.environ.get('S3_BUCKET', 'dcp-prod-owner-accelerator')
S3_SOURCE_PATH = os.environ.get('S3_SOURCE_PATH', 'pricing_processed')
S3_OUTPUT_PATH_KEY = os.environ.get('S3_OUTPUT_PATH_KEY', 'pricing_merged')
S3_OUTPUT_FILE_NAME = os.environ.get('S3_OUTPUT_FILE_NAME', 'pricing_data_merged.csv')

CSV_COLUMNS = ['garage_vehicle_id',
                'trade_in_min_price',
                'trade_in_max_price',
                'private_min_price',
                'private_max_price', 
                'car_price_kmap', 
                'price_ahead_min_price',
                'price_ahead_max_price', 
                'io_min_price' , 
                'io_max_price',
                'redbook_min_price',
                'redbook_max_price',
                'lm_retail_price',
                'load_dt']

SOURCE_FILES = {
    'livemarket': 'livemarket_pricing_1.csv',
    'redbook': 	'redbook_pricing_1.csv',
    'instantoffer': 'instantoffer_pricing_1.csv',
    'priceahead': 'priceahead_pricing_1.csv'
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

        
def merge_price_data(df: DataFrame):
    
    #pricing_columns_from_src_files = ['garage_vehicle_id', 'lm_retail_price', 'lm_min_trade', 'lm_max_trade',
    #   'lm_min_price', 'lm_max_price', 'lm_exists', 'rb_trade_in_min_price',
    #   'rb_trade_in_max_price', 'rb_private_min_price', 'rb_private_max_price',
    #   'io_min_price', 'io_max_price', 'price_ahead_min_price',
    #   'price_ahead_max_price']

    df['trade_in_min_price'] = np.where(df['lm_exists'], df['lm_min_trade'], df['rb_trade_in_min_price'])
    df['trade_in_max_price'] = np.where(df['lm_exists'], df['lm_max_trade'], df['rb_trade_in_max_price'])
    df['private_min_price'] = np.where(df['lm_exists'], df['lm_min_price'], df['rb_private_min_price'])
    df['private_max_price'] = np.where(df['lm_exists'], df['lm_max_price'], df['rb_private_max_price'])

    df['car_price_kmap'] = df['lm_exists'].astype(bool)

    df.rename(columns={'rb_private_min_price': 'redbook_min_price',
                        'rb_private_max_price': 'redbook_max_price'
                        }, inplace=True)


def handler(event, context):
    print('Event:', event)
    S3Bucket = event.get('S3Bucket') if event.get('S3Bucket') else S3_BUCKET
    S3SourcePath = event.get('S3SourcePath') if event.get('S3SourcePath') else S3_SOURCE_PATH
    S3OutputPathKey = event.get('S3OutputPathKey') if event.get('S3OutputPathKey') else S3_OUTPUT_PATH_KEY
    S3OutputFileName = event.get('S3OutputFileName') if event.get('S3OutputFileName') else S3_OUTPUT_FILE_NAME
    
    s3_source_path = f's3://{S3Bucket}/{S3SourcePath}/'
    primary_key = 'garage_vehicle_id'

    start = time.perf_counter()

    try:
        # read source files
        logger.info('Reading source files...')
        df_source = wr.s3.read_csv(s3_source_path+SOURCE_FILES['livemarket'])
        df_rb = wr.s3.read_csv(s3_source_path+SOURCE_FILES['redbook'])
        df_source = df_source.merge(df_rb, how='inner', on=primary_key, suffixes=('', '_y'))
        del df_rb
        df_io = wr.s3.read_csv(s3_source_path+SOURCE_FILES['instantoffer'])
        df_source = df_source.merge(df_io, how='inner', on=primary_key, suffixes=('', '_y'))
        del df_io
        df_pa = wr.s3.read_csv(s3_source_path+SOURCE_FILES['priceahead'])
        df_source = df_source.merge(df_pa, how='inner', on=primary_key, suffixes=('', '_y')).filter(regex='^(?!.*_y)')
        del df_pa
        
        # process
        logger.info('Merging data...')
        merge_price_data(df_source)

        #save result
        logger.info('Saving result...')
        s3_output_file = f's3://{S3Bucket}/{S3OutputPathKey}/{S3OutputFileName}'
        wr.s3.to_csv(df=df_source[CSV_COLUMNS], index=False, path=s3_output_file, 
                    sep='|', date_format='%Y-%m-%dT%H:%M:%SZ', dtype={'load_dt':'timestamp'})
        print(f'Time taken to merge pricing data: {(time.perf_counter()-start):.5f}s')

        logger.info(f'All done. File saved to {s3_output_file}')

        return {
                "success": True,
                "body": {
                    "bucket": S3Bucket,
                    "key": f'{S3OutputPathKey}/{S3OutputFileName}'
                }
            }
    
    except Exception as ex:
        logger.error(ex)
        return { "success": False }


if __name__=='__main__':
    #handler({"S3Bucket":"dcp-prod-owner-accelerator", "S3SourcePath":"pricing_processed", 
    #        "S3OutputPathKey": "pricing_merged", "S3OutputFileName": "pricing_data_merged.csv"}, '')
    print('All done.')