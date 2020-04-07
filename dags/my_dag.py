from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Matthias Engels',
    'depends_on_past':False,
    'start_date': datetime(2018, 11, 1),
    'retries':3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default' : False
}

dag = DAG('my_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context = True,
    table = 'public.staging_events',
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data/{execution_date.year}/{execution_date.month}',
    json_path = 's3://udacity-dend/log_json_path.json',
    region = 'us-west-2'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context = True,
    table = 'public.staging_songs',
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',
    json_path = 'auto',
    region = 'us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context = True,
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    sql_statement = SqlQueries.songplay_table_insert,
    table = 'public.songplays',
    mode = 'append-only'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context = True,
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    sql_statement = SqlQueries.user_table_insert,
    table = 'public.users',
    mode = 'append-only'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context = True,
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    sql_statement = SqlQueries.song_table_insert,
    table = 'public.songs',
    mode = 'append-only'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context = True,
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    sql_statement = SqlQueries.artist_table_insert,
    table = 'public.artists',
    mode = 'append-only'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context = True,
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    sql_statement = SqlQueries.time_table_insert,
    table = 'public.time',
    mode = 'append-only'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context = True,
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    table = ['public.users', 'public.songs', 'public.artists', 'public.time', 'public.songplays']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task ordering
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
