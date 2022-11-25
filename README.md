# Sparkify Song Play Data Pipelines with Airflow Project

### ***Udacity Data Engineering Course 4: Automate Data Pipelines***
### ***Final Course Project Assignment***

***
A music streaming startup (Sparkify) stores all it's key event data on S3.
The data is well structured as a Data Lake with json files.

This project implements an Airflow managed ETL pipeline to extract data from S3, then load it to Redshift, with data quality checks.
The pipeline is split into tasks that are managed by Airflow and data is backfilled, by running a day at a time for the required backfilling of one month.

The whole project is based on Airflow and custom hooks and operators for AWS, RedShift (PostgreS), S3.
***
## Contents: 
1. Data to be used: the data is stored in a S3 Bucket for Udacity
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
Made these changes after good technical feedback from the reviewer:
 1. Changed DAG schedule to hourly
 2. Used lists to group task dependencies for upstream and down stream tasks 
    instead of individually task by next task.
 3. Updated the Data Quality Operator to use enumerate() instead 
    of separately tracking the test index in the whole set of tests.
    Changed elf.log to self.log for logging if no tests were provided.
    Improved Logging if all tests passed.

***
### To run (For a local Setup, not the Udacity Environment)
### 1. update 'dwh.cfg' with the right AWS credentials and required RedShift settings
### 2. Then create and start redshift cluster with create_redshift.py module, 
###     update path to folder for sys.path accordingly for the DAG.
### 3. Start Airflow Webserver and scheduler with all the files in the dag folder, 
###    connect the cluster to PostGres or another RDMS
###    to enable parallel execution (won't happen on SQLite)
### 4. Create and test all Airflow connections to AWS, RedShift
### 5. Turn on Sparkify DAG on Airflow Web UI and monitor execution.
### 6. Wait for your Data!!!, then delete RedShift Cluster.

## Dag Graph Overview in Airflow
![Dag Image](https://raw.githubusercontent.com/obiradaniel/Sparkify-Airflow-Pipeline/main/1_Sparkify_DAG.png)

## Sparkify Redshift DW Schema (DB Beaver)
![Dag Image](https://raw.githubusercontent.com/obiradaniel/Sparkify-Airflow-Pipeline/main/2_schema_redshift_DW.png)

## Sparkify DAG RUN (SQL in validate was updated)
![Dag Image](https://raw.githubusercontent.com/obiradaniel/Sparkify-Airflow-Pipeline/main/3_Sparkify_DAG_run.png)

## Dag Graph Overview in Airflow after Successful Run 
## (SQL in validate was updated)
![Dag Image](https://raw.githubusercontent.com/obiradaniel/Sparkify-Airflow-Pipeline/main/4_Sparkify_DAG_successful_run.png)

## Red Shift Query
![Dag Image](https://raw.githubusercontent.com/obiradaniel/Sparkify-Airflow-Pipeline/main/5_redshift_query.png)


### ***Obira Daniel, November 2022***
