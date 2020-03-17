from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_count=True,
                 check_nulls=True,
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.check_count=check_count
        self.check_nulls=check_nulls
        self.tables = tables
        self.redshift_conn_id=redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator implemented and starting...')
        for table in self.tables:
            if (self.check_count):
                self.log.info(f"Checking that table: {table} has rows...")
                redshift_hook = PostgresHook(self.redshift_conn_id)
                records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {table} returned no results")
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
                
            if (self.check_nulls):
                self.log.info(f"Checking that table: {table} has no null primary key records...")
                redshift_hook = PostgresHook(self.redshift_conn_id)
                records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table} WHERE 1 IS NULL")
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {table} returned no results")
                num_records = records[0][0]
                if num_records > 0:
                    raise ValueError(f"Data quality check failed. {table} contained {num_records} rows with null values")
                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} null records")
        