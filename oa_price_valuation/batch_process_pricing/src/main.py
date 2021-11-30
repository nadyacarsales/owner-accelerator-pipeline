
from datetime import timezone
import os
import sys
import logging, logging.config
import time, datetime
import argparse
from services import pricing_client_factory
from model.pricing_client_type import PricingClientType
from services.pricing_service import PricingService
from utilities import s3_util
import pandas as pd

S3_OUTPUT_BUCKET = os.environ.get('S3_OUTPUT_BUCKET', 'dcp-prod-owner-accelerator')
S3_OUTPUT_PATH_KEY = os.environ.get('S3_OUTPUT_PATH_KEY', 'pricing_processed')
#S3_OUTPUT_FILE_NAME = os.environ.get('S3_OUTPUT_FILE_NAME', 'pricing_data.csv')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_arg():
    """
    This function parses command line arguments to this script
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--S3FileKey", type=str, default=1, required=True)
    parser.add_argument("--Service", type=str, default=2, required=True)

    params = vars(parser.parse_args())

    return params


if __name__ == '__main__':
    params = parse_arg()

    print(f"Params: {params['S3FileKey']} {params['Service']}")

    file_to_process = params['S3FileKey']
    print('Parameters: ', sys.argv)
    if not file_to_process:
        raise ValueError('Parameter is not specified (S3 File to process)')
    print(f'Processing file {file_to_process}')

    service_type_str = params['Service']
    if not service_type_str:
        raise ValueError('Service type is not specified')
    print(f'Processing data with {service_type_str} service.')
    try:
        service_type = PricingClientType(service_type_str)
    except ValueError as e:
        logger.error(f'Invalid service type: {e}')
        exit(100)

    # get data to process
    s3 = s3_util.S3Util(logger=logger)
    df = s3.read_csv(file_to_process) if file_to_process.lower().startswith('s3://') \
                else s3.read_csv(f's3://{S3_OUTPUT_BUCKET}/{file_to_process}')

    pricing_factory = pricing_client_factory.PricingClientFactory(logger)
    pricing_client = pricing_factory.get_client(service_type)

    pricing_service = PricingService(pricing_client, logger)

    # process
    t1 = time.perf_counter()
    df_orig = df.copy()
    df_final = pricing_service.process_data_in_batches(df_orig)
    t2 = time.perf_counter()
    print(f'Total time to get pricing data: {(t2-t1):.3f}s')

    df_final['load_dt'] = df_orig['load_dt'].values
    
    # save results
    output_file_name = f'{service_type.value}_pricing.csv'
    logger.info(f'Saving results to: s3://{S3_OUTPUT_BUCKET}/{S3_OUTPUT_PATH_KEY}/{output_file_name}')
    s3.write_csv(df_final[[pricing_service.PRIMARY_KEY_COLUMN] + pricing_client.csvColumns()+['load_dt']], f's3://{S3_OUTPUT_BUCKET}/{S3_OUTPUT_PATH_KEY}/{output_file_name}')
    logger.info(f'Finished processing {file_to_process}. Saved results: s3://{S3_OUTPUT_BUCKET}/{S3_OUTPUT_PATH_KEY}/{output_file_name}')
    