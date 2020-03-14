import datetime

from airflow import DAG

from airflow.operators import (
    FactsCalculatorOperator,
    HasRowsOperator,
    S3ToRedshiftOperator
)

#
# TODO: Create a DAG which performs the following functions:
#
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift
#           a. **NOTE**: to complete this step you must complete the FactsCalcuatorOperator
#              skeleton defined in plugins/operators/facts_calculator.py
#
dag = DAG("lesson3.exercise4", 
          start_date=datetime.datetime.utcnow())

#
# TODO: Load trips data from S3 to RedShift. Use the s3_key
#       "data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
#       and the s3_bucket "udacity-dend"
#
copy_trips_task = S3ToRedshiftOperator(
                        dag=dag,
                        task_id="copy_trips",
                        redshift_conn_id="redshift",
                        aws_credentials_id="aws_credentials",
                        table="trips",
                        s3_bucket="udacity-dend",
                        s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv")

#
# TODO: Perform a data quality check on the Trips table
#
check_trips = HasRowsOperator(
                dag=dag,
                task_id="check_trips",
                redshift_conn_id="redshift",
                table="trips")

#
# TODO: Use the FactsCalculatorOperator to create a Facts table in RedShift. The fact column should
#       be `tripduration` and the groupby_column should be `bikeid`
#
calculate_facts = FactsCalculatorOperator(
                dag=dag,
                task_id="calculate_facts",
                redshift_conn_id="redshift",
                origin_table="trips",
                destination_table="durations",
                fact_column="tripduration",
                groupby_column="bikeid")

#
# TODO: Define task ordering for the DAG tasks you defined
#
copy_trips_task >> check_trips
check_trips >> calculate_facts