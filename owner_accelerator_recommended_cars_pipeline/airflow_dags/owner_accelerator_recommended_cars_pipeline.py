import datetime
import os
import logging
from dateutil.tz import *

#from airflow import DAG
from airflow.models import Variable, XCom
from airflow.utils.db import provide_session
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator
from airflow.decorators import dag
from psycopg2.extras import RealDictCursor


##### TODO:
# - Send slack message in the end

SCRIPTS_BUCKET = 'csn-dcp-etl-jobs'
SCRIPTS_KEY = 'etl_ecs/owner-accelerator-recommended-cars-pipeline/etl_scripts'
S3_BUCKET = os.environ.get('S3_OUTPUT_BUCKET', 'dcp-prod-owner-accelerator')
S3_DESTINATION_KEY = os.environ.get('S3_OUTPUT_PATH_KEY', 'recommended_cars')
S3_OUTPUT_FILE_NAME = 'recommended-cars-data.csv'

rec_cars_params = Variable.get('owner_acc_recommended_cars_params', deserialize_json = True)

default_rec_cars_params = {
"S3Bucket":"dcp-prod-owner-accelerator",
"S3DestinationDataPrefix": "recommended_cars",
"S3DestinationFileKey": "recommended-cars-data.csv",
"RecommendedCarsJob": {
    "Queue": "arn:aws:batch:ap-southeast-2:555050780997:job-queue/spot-queue",
    "Definition": "arn:aws:batch:ap-southeast-2:555050780997:job-definition/owner-accelerator-populate-recommended-cars:5"
  },
"RedshiftIAMRole": "arn:aws:iam::555050780997:role/cspot.redshift.cluster"
}

def get_param(key:str, subkey:str=None):
    try:
        return rec_cars_params.get(key) if subkey is None or len(subkey)==0 else rec_cars_params.get(key).get(subkey)
    except:
        return default_rec_cars_params.get(key) if subkey is None or len(subkey)==0 else default_rec_cars_params.get(key).get(subkey)


# default args
production_args = {
    'owner': 'DCP_AIRFLOW_PROD',
    'depends_on_past': False,
    'start_date': datetime.datetime(2010, 10, 1),
    'email': ['datacollection@carsales.com.au'],
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True,
    'catchup': False
}

#---------------------------------------#

def load_script(script_path):
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    return s3_hook.read_key(bucket_name=SCRIPTS_BUCKET, key=f'{SCRIPTS_KEY}/{script_path}')

def execute_script(*op_args, **kwargs):
    script_name = op_args[0]
    print(f'Loading {script_name}. Params: {op_args[1]}, {op_args[2]}')
    script = load_script(script_name)
    if len(script)==0:
        raise ValueError(f'Script {script_name} is empty.')
    script = script.format(op_args[1], op_args[2])
    print('Loaded script. Starting to execute.')
    pg_hook = PostgresHook(postgres_conn_id='redshift_cspot')
    with pg_hook.get_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(script)
    print(f'Finished executing script {script_name}.')


def verify_loaded_data(*op_args, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='redshift_cspot')
    print('-------------------------------------------------------')
    print(f'Params: {op_args[0]}')
    query = f'''select query, trim(filename) as filename, lines_scanned, curtime, status  
            from stl_load_commits 
            where filename like '%{get_param("S3DestinationFileKey")}%' 
                and (curtime AT TIME ZONE 'UTC')>convert_timezone('UTC', cast('{op_args[0]}' as timestamp)) 
            order by curtime desc;'''
    print(f'Query to be executed: {query}')
    with pg_hook.get_conn() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            records = cursor.fetchall()
            print('1. Records found in stl_load_commits: ', records)
            if len(records)==0:
                raise ValueError(f'Data loaded with errors. Check stl_load_errors for results.')
            print(f'Description: {cursor.description}')
            print(f'Rec[0]: {records[0]} Type={type(records[0])}')
            loaded_records = records[0]["lines_scanned"]
            logging.info(f'Data loaded successfully. Rows: {loaded_records}')
            
            print('2. Checking record count')
            cursor.execute('SELECT count(1) as rec_count FROM cspot_owner_acc.recommended_cars')
            records = cursor.fetchall()
            if len(records)==0:
                raise ValueError(f'Data loaded with errors. Check stl_load_errors for results.')
            records_in_table = records[0]["rec_count"]
            if loaded_records!=records_in_table:
                raise ValueError(f'Data loaded with errors. Records in source file: {loaded_records} Records in table: {records_in_table} Check stl_load_errors for results.')
            logging.info(f'Data loaded successfully. Rows: {records_in_table}')
#---------------------------------------#

@dag(
    dag_id = 'owner_accelerator_recommended_cars',
    description = 'Loads Owner Accelerator recommended cars',
    concurrency = 1,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(hours=3),
    default_args = production_args,
    schedule_interval='0 2 * * *'
)
def owner_accelerator_recommended_cars_process():
    start_job_op = DummyOperator(
        task_id='start_job'
    )
    
    get_recommended_cars_op = AwsBatchOperator(
                task_id='get_recommended_cars',
                job_name='oa-get-recommended-cars',
                job_definition=get_param('RecommendedCarsJob', 'Definition'),
                job_queue=get_param('RecommendedCarsJob', 'Queue'),
                region_name='ap-southeast-2',
                overrides={
                    'environment': [
                    {
                        'name': "S3_OUTPUT_BUCKET",
                        'value': get_param('S3Bucket')
                    },
                    {
                        'name': "S3_OUTPUT_PATH_KEY",
                        'value': get_param('S3DestinationDataPrefix')
                    }]
                }
    )


    transfer_from_s3_to_recommended_cars_table_op = PythonOperator(
        task_id="copy_data_from_s3_to_redshift", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['recommended_cars.sql', 
                f's3://{get_param("S3Bucket")}/{get_param("S3DestinationDataPrefix")}/{get_param("S3DestinationFileKey")}', 
                get_param('RedshiftIAMRole')])


    verify_loaded_data_op = PythonOperator(
        task_id="verify_loaded_data", 
        provide_context = True,
        python_callable=verify_loaded_data,
        op_args=['{{ dag_run.start_date }}'])

    @provide_session
    def cleanup_xcom(session=None, **context):
        dag = context["dag"]
        dag_id = dag._dag_id 
        # It will delete all xcom of the dag_id
        session.query(XCom).filter(XCom.dag_id == dag_id).delete()

    cleanup_xcom_op = PythonOperator(
        task_id="cleanup_xcom",
        python_callable = cleanup_xcom,
        provide_context=True
    )

    end_job_op = DummyOperator(
        task_id='end_job'
    )

    start_job_op >> get_recommended_cars_op >> transfer_from_s3_to_recommended_cars_table_op >> \
         verify_loaded_data_op >> cleanup_xcom_op >> end_job_op

owner_accelerator_recommended_cars_dag = owner_accelerator_recommended_cars_process()