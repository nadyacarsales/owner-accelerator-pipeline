import os
import logging
import datetime
import pandas as pd
from pandas.core.frame import DataFrame
from utilities import redshift_config, settings_util, s3_util
from repository.redshift_repository import RedshiftRepository

SECRET_KEYS_PREFIX = 'redshift-cspot'
S3_OUTPUT_BUCKET = os.environ.get('S3_OUTPUT_BUCKET', 'dcp-prod-owner-accelerator')
S3_OUTPUT_PATH_KEY = os.environ.get('S3_OUTPUT_PATH_KEY', 'pricing_data_to_process')
IS_INCREMENTAL = os.environ.get('IS_INCREMENTAL', False)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PRICING_DATA_FULL_QUERY = "select garage_vehicle_id , spot_id, redbook_key, redbookcodelegacy, kms, getdate() as load_dt  " \
                        "from ( select a.garage_vehicle_id , A.spot_id, B.REDBOOK_KEY, B.redbookcodelegacy, A.kms " \
                        "FROM cspot_owner_acc.my_garage A, " \
                        "cspot_landing.xx_csn_redbook_d B " \
                        " WHERE lower(A.SPOT_ID)=lower(B.SPOT_ID) " \
                        " and A.kms is not null  and cast (A.kms as integer) > 9 order by a.garage_vehicle_id); "

PRICING_DATA_INCREMENTAL_QUERY = "select garage_vehicle_id , spot_id, redbook_key, redbookcodelegacy, kms, getdate() as load_dt  " \
                        "from ( select a.garage_vehicle_id , A.spot_id, B.REDBOOK_KEY, B.redbookcodelegacy, A.kms " \
                        "FROM cspot_owner_acc.my_garage A, " \
                        "cspot_landing.xx_csn_redbook_d B " \
                        " WHERE lower(A.SPOT_ID)=lower(B.SPOT_ID) " \
                        " and A.kms is not null  and cast (A.kms as integer) > 9 "\
                        " and (odometer_update_dt > (select isnull(max(dateadd(day, -1, load_dt)), '1900-01-01')  from cspot_owner_acc.car_valuation) " \
 		                " or garage_ad_date > (select isnull(max(dateadd(day, -1, load_dt), '1900-01-01') from cspot_owner_acc.car_valuation)) " \
                        " order by a.garage_vehicle_id); "


def fetch_data(query) -> DataFrame:
    """
    Retrieves data from database.
    """
    cspot_config = redshift_config.RedshiftConfig(settings_util.ParameterStoreSettings(logger), 
                                                    params_prefix=SECRET_KEYS_PREFIX)
    repo = RedshiftRepository(cspot_config, logger)
    repo.init_connection()
    
    df = None
    for df_iter in repo.execute_query_fetch_data(query, chunksize=10000):
        for d in df_iter:
            df = pd.concat([df,d], ignore_index=True)
    
    return df


def handler(event, context):
    print('Event:', event)
    S3Bucket = event.get('S3Bucket') if event.get('S3Bucket') else S3_OUTPUT_BUCKET
    S3OutputKey = event.get('S3OutputKey') if event.get('S3OutputKey') else S3_OUTPUT_PATH_KEY
    is_incremental = True if (event.get('IsIncremental') and str(event.get('IsIncremental')).lower()=='true') else IS_INCREMENTAL
    print(f'Bucket: {S3Bucket}, OutputKey: {S3OutputKey}')
    if not S3Bucket:
        raise ValueError('Input parameter (S3Bucket) is not specified')
    
    dt = datetime.datetime.now()
    output_file_name = dt.strftime("%Y_%m_%d_%H_%M_%S")+'.csv'
    full_s3_path = f's3://{S3Bucket}/{S3OutputKey}/{output_file_name}'
    try:
        #dt_string = dt.strftime("%Y-%m-%d")
        df = fetch_data(PRICING_DATA_INCREMENTAL_QUERY if is_incremental else PRICING_DATA_FULL_QUERY)
        if df is None:
            logger.warn('No data retrieved from db.')
            return { "success": False }

        s3_util.S3Util().write_csv(df, full_s3_path)
        return {
            "success": True,
            "body": {
                "bucket": S3Bucket,
                "key": f'{S3OutputKey}/{output_file_name}',
                "full_s3_path": full_s3_path
            }
        }

    except Exception as ex:
        logger.error(ex)
        return { "success": False }
    

if __name__ == '__main__':
    #handler({"S3Bucket":"dcp-prod-owner-accelerator", "S3OutputKey":"pricing_data_to_process", "IsIncremental": "false"}, '')
    print('All done.')