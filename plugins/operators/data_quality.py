from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials = "",
                 table = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table

    def execute(self, context):
        '''
        This Operator execute a function that check if the tables are containing records.
        Arguments are passed from DAG.

        args:{
         : redshift_conn_id = parameters of the redshift connection
         : aws_credentials = AWS credentials
         : table = dict of table names
        }
        '''
        redshift_hook = PostgresHook("redshift")

        for table in self.table:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            num_records = records[0][0]
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            elif num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            else:
                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
