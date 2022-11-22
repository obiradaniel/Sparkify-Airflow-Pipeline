# Sparkify Song Play Data Pipelines with Airflow Project

### ***Udacity Data Engineering Course 4: Automate Data Pipelines***
### ***Final Course Project Assignment***

***
A music streaming startup (Sparkify) stores all it's key event data on S3.
The data is well structured as a Data Lake with json files.

This project implements an ETL pipeline to extract data from S3, then load it to Redshift, with data quality checks.
The pipeline is split into tasks that are managed by Airflow and data is backfilled by running a month at a time with flexible scheduling.

The whole project is based on Airflow and custom hooks and operators for AWS, RedShift, S3..
***
## Contents: 
1. Data to be used: the data is stored in a S3 Bucket
    1. Listening events for it's streaming data as JSON logfiles.
    2. Song data for all tracks available on it's service as JSON files. 
    
2. Python Scripts in Folder root and order of execution
 
    1. **etl.py** - loads data from S3 to EMR cluster or pyspark, processes the data into analytic tables then exports the ready tables back to S3 as parquet files.

3. dl.cfg - Contains AWS credentials and EMR Cluster parameters.

***
### To run first update dl.cfg, then run ETL.



### ***Obira Daniel, September 2022***
