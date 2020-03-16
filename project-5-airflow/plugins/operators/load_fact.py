from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 delete_flag=False,
                 columns="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql=sql
        self.table=table
        self.delete_flag=delete_flag
        self.columns=columns

    def execute(self, context):
        self.log.info("Loading the fact table {} in progress".format(
                        self.table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if(self.delete_flag):
            redshift.run("DELETE FROM {}".format(self.table))
        
        full_sql = "INSERT INTO {} {} ".format(self.table, self.columns)
        full_sql += self.sql
        redshift.run(full_sql)
