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
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False, 
    'retries': 3,
    'retry_delta': timedelta(minutes=5),
    'email_on_retry': False, 
    'catchup' : False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/",
    table='staging_events',
    json_load_option='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/",
    table='staging_songs',
    json_load_option='auto'
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag, 
    redshift_conn_id="redshift",
    redshift_database_schema="public", 
    fact_table='songplays',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    redshift_database_schema="public",
    dim_table='users',
    sql_query=SqlQueries.user_table_insert,
    truncate_before_insert=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    redshift_database_schema="public",
    dim_table='songs',
    sql_query=SqlQueries.song_table_insert,
    truncate_before_insert=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    redshift_database_schema="public",
    dim_table='artists',
    sql_query=SqlQueries.artist_table_insert,
    truncate_before_insert=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    redshift_database_schema="public",
    dim_table='time',
    sql_query=SqlQueries.time_table_insert,
    truncate_before_insert=True
)

tests = [
        {
            'sql_test':'select count(*) from songplays where playid is NULL',
            'expected_result': 0
        },
        {
            'sql_test':'select count(*) from songplays where start_time is NULL',
            'expected_result': 0
        },
        {
            'sql_test':'select count(*) from songplays where userid is NULL',
            'expected_result': 0
        },
        {
            'sql_test':"select count(*) from songplays where level not in ('paid', 'free')",
            'expected_result': 0
        },
        {
            'sql_test':'select count(*) from songs where songid is NULL',
            'expected_result': 0
        },
        {
            'sql_test':'select count(*) from artists where artistid is NULL',
            'expected_result': 0
        },
        {
            'sql_test':'select count(*) from users where userid is NULL',
            'expected_result': 0
        },
        {
            'sql_test':'select count(*) from time where start_time is NULL',
            'expected_result': 0
        },
        {
            'sql_test':'select count(*) from songplays where songid not in (select distinct songid from songs)',
            'expected_result': 0
        },
        {
            'sql_test':'select count(*) from songplays where artistid not in (select distinct artistid from artists)',
            'expected_result': 0
        },
    ]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    data_quality_checks = tests
    )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table 
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator 

