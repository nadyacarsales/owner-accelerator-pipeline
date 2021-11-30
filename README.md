# owner-accelerator-pipeline
Pipeline for populating cspot_owner_acc. Documentation here: https://carsales.atlassian.net/wiki/spaces/DE/pages/1720254502/Project+description+and+architecture

# Code structure

## airflow
Contains Airflow DAG and sql scripts used by the DAG

## oa_price_valuation
Code for populating *car_valuation* table

### lambda_init_processing_data
Selects garage items for be processed and saves data to S3.

### batch_process_pricing
Gets data file from S3, calls API (supports 4 pricing APIs, API type is passed as input parameter) and saves result to S3.

### lambda_price_valuation_merge
1. Gets data files saved by batch job (pricing data from APIs)
2. Applies pricing logic (see https://carsales.atlassian.net/wiki/spaces/DE/pages/1541865794/Owner+Accelerator) and merges all into one DataFrame
3. Saves result to S3

### orchestration
Step Function code. This Step Function orchestrates the car valuation flow.
*input_params.json* - sample input parameters for Step Function.

## oa_recommended_cars
Code for populating *recommended_cars* table.

## owner_accelerator_pricing_pipeline
Airflow DAG for car valuation pipeline. This is the same code as in the main DAG. Can be used for testing car valuation pipeline separately from other OA components.

## owner_accelerator_recommended_cars_pipeline
Airflow DAG for recommended cars pipeline. This is the same code as in the main DAG. Can be used for testing recommended cars pipeline separately from other OA components.