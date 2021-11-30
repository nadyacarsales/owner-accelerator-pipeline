import datetime
import time
from dateutil.tz import *
import os
import logging


from airflow.models import Variable, XCom
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
#from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator
from airflow.providers.amazon.aws.operators.step_function_start_execution import StepFunctionStartExecutionOperator
from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.db import provide_session
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from psycopg2.extras import RealDictCursor

##### TODO:
# 1. Move parametrs to variables
# 2. Add slack message when job fails
# 3. Add quality checks
# 4. Use custom operator (DevOps to add fetching all folders/files inside root folder to put on Airflow server)

SCRIPTS_BUCKET = 'csn-dcp-etl-jobs'
SCRIPTS_KEY = 'etl_ecs/owner-accelerator-pipeline/etl_scripts'
S3_BUCKET = os.environ.get('S3_OUTPUT_BUCKET', 'dcp-prod-owner-accelerator')

owner_acc_etl_params = Variable.get('owner_acc_etl_params', deserialize_json = True)

rec_cars_params = owner_acc_etl_params['owner_acc_recommended_cars_params']
print(rec_cars_params)
default_rec_cars_params = {
"S3Bucket":"dcp-prod-owner-accelerator",
"S3DestinationDataPrefix": "recommended_cars",
"S3OutputFileName": "recommended-cars-data.csv",
"RecommendedCarsJob": {
    "Queue": "arn:aws:batch:ap-southeast-2:555050780997:job-queue/spot-queue",
    "Definition": "arn:aws:batch:ap-southeast-2:555050780997:job-definition/owner-accelerator-populate-recommended-cars:5"
  },
"RedshiftIAMRole": "arn:aws:iam::555050780997:role/cspot.redshift.cluster"
}

pricing_params = owner_acc_etl_params['owner_acc_pricing_params']

default_pricng_params = {
"S3Bucket":"dcp-prod-owner-accelerator",
"S3SourceDataOutputKey": "pricing_data_to_process", 
"S3PricingApiDataOutputKey": "pricing_processed",
"S3AggregatedDataPath": "pricing_merged",
"S3OutputFileName": "pricing_data_merged.csv",
"IsIncremental": False,
"PriceValuationJob": {
    "Queue": "arn:aws:batch:ap-southeast-2:555050780997:job-queue/spot-queue",
    "Definition": "arn:aws:batch:ap-southeast-2:555050780997:job-definition/owner-accelerator-populate-pricing-from-api:2"
  },
"StepFunctionsArn": "arn:aws:states:ap-southeast-2:555050780997:stateMachine:OwnerAcceleratorPriceValuation",
"RedshiftIAMRole": "arn:aws:iam::555050780997:role/cspot.redshift.cluster"
}

def get_param(params_dict:dict, default_params_dict:dict, key:str, subkey:str=None):
    try:
        return params_dict.get(key) if subkey is None or len(subkey)==0 else params_dict.get(key).get(subkey)
    except:
        return default_params_dict.get(key) if subkey is None or len(subkey)==0 else default_params_dict.get(key).get(subkey)

def get_rec_cars_param(key:str, subkey:str=None):
    return get_param(rec_cars_params, default_rec_cars_params, key, subkey)

def get_car_valuation_param(key:str, subkey:str=None):
    return get_param(pricing_params, default_pricng_params, key, subkey)

# default args
production_args = {
    'owner': 'DCP_AIRFLOW_PROD',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 11, 23),
    'email': ['datacollection@carsales.com.au'],
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}


#---------------------------------------#

def load_script(script_path):
    s3_hook = S3Hook(aws_conn_id='aws_csn_dcp')
    return s3_hook.read_key(bucket_name=SCRIPTS_BUCKET, key=f'{SCRIPTS_KEY}/{script_path}')

def execute_script(*op_args, **kwargs):
    script_name = op_args[0]
    print(f'Loading {script_name}')
    script = load_script(script_name)
    if len(script)==0:
        raise ValueError(f'Script {script_name} is empty.')
    print('Loaded script. Starting to execute.')
    pg_hook = PostgresHook(postgres_conn_id='redshift_cspot')
    with pg_hook.get_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(script)
    print(f'Finished executing script {script_name}.')

def execute_copy_to_redshift_script(*op_args, **kwargs):
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

def wait_for_step_functions_completion(**kwargs):
    '''
    **kwargs: dict containing step function execution arn
    '''
    execution_id = kwargs['execution_id']
    print('Execution_id:', execution_id)
    sfn_hook = StepFunctionHook(aws_conn_id='aws_csn_dcp')
    #finished_statuses = ['SUCCEEDED','FAILED','TIMED_OUT','ABORTED']
    time.sleep(30)
    execution = sfn_hook.describe_execution(execution_id)
    print('wait_for_step_functions_completion. Execution: ', execution)
    while execution['status']=='RUNNING':
        time.sleep(60)
        execution = sfn_hook.describe_execution(execution_id)
        print('Polling for step function status. Status=', execution['status'])
    return execution['status']


def verify_car_valuation_task_succeeded(**kwargs):
    '''
    Checks if step function execution has succeeded. If not, raises exception
    Status values: 'RUNNING'|'SUCCEEDED'|'FAILED'|'TIMED_OUT'|'ABORTED'
    '''
    if kwargs['sfn_result'] not in ('RUNNING', 'SUCCEEDED', 'ABORTED'):
        raise AirflowException(f"Car valuation step functions did not succeed. Status: {kwargs['sfn_result']}")
    

def check_if_incremental(*op_args, **kwargs):
    return 'car_valuation_task_group.copy_data_from_s3_to_redshift_incremental' \
        if str(get_car_valuation_param('IsIncremental')).lower()=='true' \
            else 'car_valuation_task_group.copy_data_from_s3_to_redshift'


def verify_loaded_data(*op_args, **kwargs):
    '''
    Checks if data from the specified file was successfully loaded into Redshift.
    :param: kwargs: dt, file_name, table_name
    '''
    pg_hook = PostgresHook(postgres_conn_id='redshift_cspot')
    print('-------------------------------------------------------')
    print(f'Kwargs: {kwargs}')
    query = f'''select query, trim(filename) as filename, lines_scanned, curtime, status  
            from stl_load_commits 
            where filename like '%{kwargs["file_name"]}%' 
                and (curtime AT TIME ZONE 'UTC')>convert_timezone('UTC', cast('{kwargs["dt"]}' as timestamp)) 
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
            cursor.execute(f'SELECT count(1) as rec_count FROM cspot_owner_acc.{kwargs["table_name"]}')
            records = cursor.fetchall()
            if len(records)==0:
                raise ValueError(f'Data loaded with errors. Check stl_load_errors for results.')
            records_in_table = records[0]["rec_count"]
            if loaded_records!=records_in_table:
                print(f'Records in source file: {loaded_records} Records in table: {records_in_table} Check stl_load_errors for results.')
                #raise ValueError(f'Data loaded with errors. Records in source file: {loaded_records} Records in table: {records_in_table} Check stl_load_errors for results.')
            logging.info(f'Data loaded successfully. Rows: {records_in_table}')

#------------- DAG -------------
@dag(
    dag_id='owner_accelerator_etl',
    description='Loads CSPOT Owner Accelerator tables',
    concurrency=20,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(hours=3),
    default_args = production_args,
    schedule_interval='30 0 * * *'
)

def owner_accelerator_etl():
    start_job_op = DummyOperator(
        task_id='start_job'
    )

    populate_my_garage_table = PythonOperator(
        task_id="populate_my_garage_table", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['my_garage.sql'])

    populate_similar_cars_table = PythonOperator(
        task_id="populate_similar_cars_table", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['similar_cars.sql'])

    populate_price_compare_table = PythonOperator(
        task_id="populate_price_compare_table", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['price_compare.sql'])

    populate_price_compare_agg_table = PythonOperator(
        task_id="populate_price_compare_agg_table", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['price_compare_agg.sql'])

    populate_car_demand_view_table = PythonOperator(
        task_id="populate_car_demand_view_table", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['car_demand_view.sql'])

    populate_car_demand_prev_table = PythonOperator(
        task_id="populate_car_demand_prev_table", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['car_demand_prev.sql'])

    populate_car_demand_leads_table = PythonOperator(
        task_id="populate_car_demand_leads_table", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['car_demand_leads.sql'])

    populate_car_demand_inv_table = PythonOperator(
        task_id="populate_car_demand_inv_table", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['car_demand_inv.sql'])

    populate_car_demand_hist_table = PythonOperator(
        task_id="populate_car_demand_historical_table", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['car_demand_historical.sql'])

    populate_car_demand_present_table = PythonOperator(
        task_id="populate_car_demand_present_table", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['car_demand_present.sql'])

    populate_car_demand_table = PythonOperator(
        task_id="populate_car_demand_table", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['car_demand.sql'])
        

    #----------- Recommended Cars -----------

    with TaskGroup(group_id='recommended_cars_task_group') as recommended_cars_task_group:
        get_recommended_cars_op = AwsBatchOperator(
                    task_id='get_recommended_cars',
                    job_name='oa-get-recommended-cars',
                    job_definition=get_rec_cars_param('RecommendedCarsJob', 'Definition'),
                    job_queue=get_rec_cars_param('RecommendedCarsJob', 'Queue'),
                    region_name='ap-southeast-2',
                    aws_conn_id='aws_csn_dcp',
                    overrides={
                        'environment': [
                        {
                            'name': "S3_OUTPUT_BUCKET",
                            'value': get_rec_cars_param('S3Bucket')
                        },
                        {
                            'name': "S3_OUTPUT_PATH_KEY",
                            'value': get_rec_cars_param('S3DestinationDataPrefix')
                        }]
                    }
        )

        transfer_from_s3_to_recommended_cars_table_op = PythonOperator(
            task_id="copy_data_from_s3_to_redshift", 
            provide_context = True,
            python_callable=execute_copy_to_redshift_script,
            op_args=['recommended_cars.sql', 
                    f's3://{get_rec_cars_param("S3Bucket")}/{get_rec_cars_param("S3DestinationDataPrefix")}/{get_rec_cars_param("S3OutputFileName")}', 
                    get_rec_cars_param('RedshiftIAMRole')])

        verify_loaded_data_op = PythonOperator(
            task_id="verify_recommended_cars_loaded_data", 
            provide_context = True,
            python_callable=verify_loaded_data,
            op_kwargs={'dt':'{{ dag_run.start_date }}', 'file_name':get_rec_cars_param("S3OutputFileName"), 'table_name':'recommended_cars'}
            )

        get_recommended_cars_op >> transfer_from_s3_to_recommended_cars_table_op >> verify_loaded_data_op
    
    #----------- Car Valuation -----------
    with TaskGroup(group_id='car_valuation_task_group') as car_valuation_task_group:
        start_step_func_op = StepFunctionStartExecutionOperator(
            task_id = "start_prepare_price_valuation_data_step_func",
            state_machine_arn = get_car_valuation_param('StepFunctionsArn'),
            aws_conn_id='aws_csn_dcp',
            state_machine_input = {
                "S3Bucket": get_car_valuation_param('S3Bucket'),
                "S3SourceDataOutputKey": get_car_valuation_param('S3SourceDataOutputKey'), 
                "S3PricingApiDataOutputKey": get_car_valuation_param('S3PricingApiDataOutputKey'),
                "S3AggregatedDataPath": get_car_valuation_param('S3AggregatedDataPath'),
                "S3OutputFileName": get_car_valuation_param('S3OutputFileName'), 
                "IsIncremental": True if (get_car_valuation_param('IsIncremental') and get_car_valuation_param('IsIncremental').lower=='true') else get_car_valuation_param('IsIncremental')
                }
        )

        wait_for_car_valuation_task_completion_op = PythonOperator(
            task_id="wait_for_car_valuation_task_completion", 
            provide_context = True,
            python_callable=wait_for_step_functions_completion,
            op_kwargs={'execution_id': '{{ti.xcom_pull(task_ids=\'car_valuation_task_group.start_prepare_price_valuation_data_step_func\')}}' }
        )        

        verify_car_valuation_task_succeeded_op = PythonOperator(
            task_id="verify_car_valuation_task_succeeded", 
            provide_context = True,
            python_callable=verify_car_valuation_task_succeeded,
            op_kwargs={'sfn_result': '{{ti.xcom_pull(task_ids=\'car_valuation_task_group.wait_for_car_valuation_task_completion\')}}' }
        )

        if_incremental_branch = BranchPythonOperator(
            task_id='if_incremental_branch',
            python_callable=check_if_incremental,
            do_xcom_push=False
        )

        copy_data_from_s3_to_redshift_incremental_op = PythonOperator(
            task_id="copy_data_from_s3_to_redshift_incremental", 
            provide_context = True,
            python_callable=execute_copy_to_redshift_script,
            op_args=['car_valuation_incremental.sql', 
                    f's3://{get_car_valuation_param("S3Bucket")}/{get_car_valuation_param("S3AggregatedDataPath")}/{get_car_valuation_param("S3OutputFileName")}', 
                    get_car_valuation_param('RedshiftIAMRole')])


        copy_data_from_s3_to_redshift_op = PythonOperator(
            task_id="copy_data_from_s3_to_redshift", 
            provide_context = True,
            python_callable=execute_copy_to_redshift_script,
            op_args=['car_valuation_full.sql', 
                    f's3://{get_car_valuation_param("S3Bucket")}/{get_car_valuation_param("S3AggregatedDataPath")}/{get_car_valuation_param("S3OutputFileName")}', 
                    get_car_valuation_param('RedshiftIAMRole')])


        verify_loaded_data_op = PythonOperator(
            task_id="verify_car_valuation_loaded_data", 
            provide_context = True,
            python_callable=verify_loaded_data,
            trigger_rule='none_failed_or_skipped',
            op_kwargs={'dt':'{{ dag_run.start_date }}', 'file_name':get_car_valuation_param("S3OutputFileName"), 'table_name':'car_valuation'}
            )

        start_step_func_op >> wait_for_car_valuation_task_completion_op >> verify_car_valuation_task_succeeded_op >> \
            if_incremental_branch >> \
            [copy_data_from_s3_to_redshift_op, copy_data_from_s3_to_redshift_incremental_op] >> \
         verify_loaded_data_op

    #----------- Aggregate results -----------

    populate_oa_combined_table = PythonOperator(
        task_id="populate_oa_combined_table", 
        provide_context = True,
        python_callable=execute_script,
        op_args=['cspot_oa_combined.sql'])

    #----------- Cleanup -----------
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

    # Create main tables
    start_job_op >> populate_my_garage_table >> [populate_similar_cars_table, populate_price_compare_table]
    start_job_op >> [populate_car_demand_view_table, populate_car_demand_prev_table, \
        populate_car_demand_leads_table, populate_car_demand_inv_table] >> populate_car_demand_hist_table \
            >> populate_car_demand_present_table >> populate_car_demand_table >> populate_oa_combined_table
    populate_my_garage_table >> populate_car_demand_hist_table
    populate_price_compare_table >> populate_price_compare_agg_table >> populate_oa_combined_table
    populate_similar_cars_table >> populate_oa_combined_table
    populate_oa_combined_table >> cleanup_xcom_op >> end_job_op

    # Recommended cars
    populate_my_garage_table >> recommended_cars_task_group >> populate_oa_combined_table

    # Car valuation pricing data
    populate_my_garage_table >> car_valuation_task_group >> populate_oa_combined_table

owner_accelerator_etl_dag = owner_accelerator_etl()