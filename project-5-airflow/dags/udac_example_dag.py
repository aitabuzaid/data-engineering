from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'aitabuzaid',
    'depends_on_past' : False,
    'start_date': datetime(2020, 3, 13),
    'email' : ['abuzaid.ait@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
}
# 'start_date': datetime(2019, 1, 12),
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup=False,
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift")

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    aws_region='us-west-2',
    json_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/",
    aws_region='us-west-2',
    json_path='auto')

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert,
    table="songplays",
    delete_flag=False,
    columns="""(playid,
                start_time,
                userid,
                level,
                songid,
                artistid,
                sessionid,
                location,
                user_agent)""")

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.user_table_insert,
    table="users",
    delete_flag=True,
    columns="""(userid,
                first_name,
                last_name,
                gender,
                level)""")

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.song_table_insert,
    table="songs",
    delete_flag=True,
    columns="""(songid,
                title,
                artistid,
                year,
                duration)""")
    


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.artist_table_insert,
    table="artists",
    delete_flag=True,
    columns="""(artistid,
                name,
                location,
                lattitude,
                longitude)""")


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.time_table_insert,
    table="time",
    delete_flag=True,
    columns="""(start_time,
                hour,
                day,
                week,
                month,
                year,
                weekday)""")


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["staging_events",
            "staging_songs",
            "artists",
            "songs",
            "songplays",
            "users",
            "time"],
    check_nulls=True,
    check_count=True
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables

create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator