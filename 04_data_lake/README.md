## Project 4: Data Lake

<p>The main goal of this project is build and ETL pipeline for a data lake hosted on S3, which will be part of Sparkify's streaming app solution.</p>

### ETL Pipeline

<p> The ETL pipeline was built step by step on a notebook and after that converted to a python script. The pipeline reads data from Udacity's S3, process it and then save it into another S3 using parquet format.</p>

The pipeline contains the files below:

- dl.cfg: file which contains aws credentials. This file is submitted not filled.
- etl.ypynb: python notebook that contains all steps of the ETL process
- et1.py: script python that contains all steps of the ETL process
- delete_parquet.sh: script that delete local parquets (small data local test)

## DATA LAKE SOLUTION

<p> The Data lake for this solution was built using AWS product below:</p>

- AWS EMR - solution to process data read from Udacity's S3 
- AWS S3 - solution to store data processed by EMR

###  ETL RESULT

<p> All results are stored at https://s3.console.aws.amazon.com/s3/buckets/udacity-project-bucket/?region=us-east-1

The tables below are stored as parquet files:

- songplays
- time
- artists
- songs
- time
- user
