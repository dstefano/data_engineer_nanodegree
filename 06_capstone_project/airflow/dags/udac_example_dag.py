from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    #'start_date': datetime(2019, 1, 12),
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
        catchup=False,
        default_args=default_args,
        description='Load and transform data in Redshift with Airflow',
        schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_i94_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94',
    dag=dag,
    table="staging_i94",
    format="parquet",
    source_file="",
    ignore_headers=1,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    iam_role = "MY_IAM_ROLE",
    s3_bucket="udacity-capstone-2020-3",
    s3_key="parquets/capstone_final"
)

stage_airports_to_redshift = StageToRedshiftOperator(
    task_id='Stage_airports',
    dag=dag,
    table="staging_airports",
    format="csv",
    source_file="/airports.csv",
    ignore_headers=1,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-2020-3",
    s3_key="csv_files"
)

stage_countries_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94_countries',
    dag=dag,
    table="staging_countries",
    format="csv",
    source_file="/countries.csv",
    ignore_headers=1,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-2020-3",
    s3_key="csv_files"
)

stage_states_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94_states',
    dag=dag,
    table="staging_states",
    format="csv",
    source_file="/states.csv",
    ignore_headers=1,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-2020-3",
    s3_key="csv_files"
)

stage_ports_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94_entry_ports',
    dag=dag,
    table="staging_ports",
    format="csv",
    source_file="/entry_ports.csv",
    ignore_headers=1,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-2020-3",
    s3_key="csv_files"
)

stage_visa_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94_visa',
    dag=dag,
    table="staging_visa",
    format="csv",
    source_file="/visa.csv",
    ignore_headers=1,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-2020-3",
    s3_key="csv_files"
)

stage_travel_mode_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94_travel_mode',
    dag=dag,
    table="staging_travel_mode",
    format="csv",
    source_file="/travel_modes.csv",
    ignore_headers=1,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-capstone-2020-3",
    s3_key="csv_files"
)



load_immigration_table = LoadFactOperator(
    task_id='Load_immigration_fact_table',
    dag=dag,
    table="immigrations",
    sql=SqlQueries.immigration_table_insert,
    redshift_conn_id="redshift"
)

load_airport_dimension_table = LoadDimensionOperator(
    task_id='Load_airport_dim_table',
    dag=dag,
    table="airport",
    redshift_conn_id="redshift",
    delete_load = True,
    sql=SqlQueries.airport_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    delete_load = True,
    sql=SqlQueries.time_table_insert
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables = ['immigrations','airport','time']

)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task ordering for the DAG tasks 
#
start_operator >> stage_i94_to_redshift
start_operator >> stage_airports_to_redshift
start_operator >> stage_countries_to_redshift
start_operator >> stage_states_to_redshift
start_operator >> stage_ports_to_redshift
start_operator >> stage_visa_to_redshift
start_operator >> stage_travel_mode_to_redshift

stage_i94_to_redshift >> load_immigration_table
stage_airports_to_redshift >> load_immigration_table
stage_countries_to_redshift >> load_immigration_table
stage_states_to_redshift >> load_immigration_table
stage_ports_to_redshift >> load_immigration_table
stage_visa_to_redshift >> load_immigration_table
stage_travel_mode_to_redshift >> load_immigration_table

load_immigration_table >> load_airport_dimension_table
load_immigration_table >> load_time_dimension_table

load_airport_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator