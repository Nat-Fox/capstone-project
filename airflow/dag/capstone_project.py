from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 8, 8),   
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('capstone_project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          #schedule_interval=None,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_checkouts_to_redshift = StageToRedshiftOperator(
    task_id='Stage_checkouts',
    dag=dag,
    provide_context=True,
    params={
        'target_table': 'staging_checkout',
        's3_bucket': 's3://cap-proj-ds/checkouts',
        'aws_credentials': {
            'AWS_KEY': AWS_KEY,
            'AWS_SECRET': AWS_SECRET,
        },
        'format': 'csv',
    }
)

stage_visits_to_redshift = StageToRedshiftOperator(
    task_id='Stage_visits',
    dag=dag,
    provide_context=True,
    params={
        'target_table': 'staging_visit',
        's3_bucket': 's3://cap-proj-ds/visits',
        'aws_credentials': {
            'AWS_KEY': AWS_KEY,
            'AWS_SECRET': AWS_SECRET
        },
        'format': 'json'
    }
)

load_time_window_met_table = LoadFactOperator(
    task_id='time_window_met_fact_table',
    dag=dag,
    provide_context=True,
    params={
        'target_table': 'time_window_met',
        'sql': SqlQueries.time_window_met_table_insert,
        'insert_fields': ['time_window_id', 'checkout_time_id', 'checkout_time', 'account_id', 'visit_id', 'met']
    }    
)

load_time_window_table = LoadDimensionOperator(
    task_id='time_window_fact_table',
    dag=dag,
    provide_context=True,
    params={
        'target_table': 'time_window',
        'sql': SqlQueries.time_window_table_insert,
        'append_data': False,
        'insert_fields': ['window_start', 'window_end', 'window_start_2', 'window_end_2', 'planned_date', 'account_id', 'visit_id']
    }
)

load_checkout_time_dimension_table = LoadDimensionOperator(
    task_id='Load_checkout_time_dim_table',
    dag=dag,
    provide_context=True,
    params={
        'target_table': 'checkout_time',
        'sql': SqlQueries.checkout_time_table_insert,
        'append_data': False,
        'insert_fields': ['checkout_time', 'visit_id']
    }
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    params={
        'sql_checks': [
            {
                'check_sql': 'SELECT COUNT(*) FROM time_window WHERE time_window_id is null',
                'operation': 'eq',
                'expected': 0,
            },{
                'check_sql': 'SELECT COUNT(*) FROM checkout_time WHERE checkout_time_id is null',
                'operation': 'eq',
                'expected': 0,
            },{
                'check_sql': 'SELECT COUNT(*) FROM time_window_met WHERE time_window_met_id is null',
                'operation': 'eq',
                'expected': 0,
            },{
                'check_sql': 'SELECT count(*) FROM time_window WHERE (window_start IS NOT NULL AND window_end IS NOT NULL) OR (window_start_2 IS NOT NULL AND window_end_2 IS NOT NULL)',
                'operation': 'gt',
                'expected': 0,
            }
        ]
    }
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_checkouts_to_redshift
start_operator >> stage_visits_to_redshift

stage_visits_to_redshift >> load_time_window_table
stage_checkouts_to_redshift >> load_checkout_time_dimension_table

load_checkout_time_dimension_table >> load_time_window_met_table
load_time_window_table >> load_time_window_met_table

load_time_window_met_table >> run_quality_checks

run_quality_checks >> end_operator
