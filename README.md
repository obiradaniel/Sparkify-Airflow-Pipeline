# Sparkify Song Play Data Pipelines with Airflow Project

### ***Udacity Data Engineering Course 4: Automate Data Pipelines***
### ***Final Course Project Assignment***

***
A music streaming startup (Sparkify) stores all it's key event data on S3.
The data is well structured as a Data Lake with json files.

This project implements an Airflow managed ETL pipeline to extract data from S3, then load it to Redshift, with data quality checks.
The pipeline is split into tasks that are managed by Airflow and data is backfilled,
by running a day at a time for the required backfilling of one month.

The whole project is based on Airflow and custom hooks and operators for AWS, RedShift, S3..
***
## Contents: 
1. Data to be used: the data is stored in a S3 Bucket
    1. Listening events for it's streaming data as JSON logfiles.
    2. Song data for all tracks available on it's service as JSON files. 
    
2. Custom Air Flow Operators (In operators subfolder)
 
    1. Stage Operator (stage_redshift.py) - loads data from S3 to RedShift
     from raw JSON Logs and Songfiles
    2. Fact Operator (load_fact.py) - loads data from RedShift Staging tables to facts tables
    3. Dimension Operator (load_dimension.py) - loads data from RedShift Staging tables to Dimension tables
    4. Data Quality Operator (data_quality.py) - tests against a given list of tests with
    expected values and raises an Error if values differ.

3. Sparkify DAG (sparkify_dag.py)
Initializes all the sub-tasks and creates dependencies thus connects tasks to
another to create the whole pipeline.

***
### To run
### 1. update 'dwh.cfg' with the right AWS credentials and required RedShift settings
### 2. then create and start redshift cluster with create_redshift.py module
### 3. Start Airflow Webserver and scheduler with all the files in the dag folder
### 4. Create and test all Airflow connections to AWS, RedShift
### 5. Turn on Sparkify DAG on Airflow Web UI.
### 6. Wait for your Data!!!.




### ***Obira Daniel, November 2022***
