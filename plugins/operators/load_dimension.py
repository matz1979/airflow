from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials = "",
                 sql_statement = "",
                 table = "",
                 mode = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.sql_statement = sql_statement
        self.table = table
        self.mode = mode

    def execute(self, context):
        '''
        This Operator execute a SQL statement to load(append-only) or delete the content of the tables and load the new content(delete-load)         four dimension tables on a AWS Redshift Cluster.
        Arguments are passed from the DAG

        args:{
          : redshift_conn_id = parameters of the redshift connection
          : aws_credentials = AWS credentials
          : sql_statement = SQL statement
          : table = table names
          : mode = append-only or delete-load
        }
        '''
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if self.mode == 'append-only':
            redshift.run(self.sql_statement)
        else:
            redshift.run(f'TRUNCATE TABLE {self.table}')
            redshift.run(self.sql_statement)
