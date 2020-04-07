from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials = "",
                 sql_statement = "",
                 table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.sql_statement = sql_statement
        self.table = table

    def execute(self, context):
        '''
        This Operator execute a SQL statement to create the fact table on the AWS Redshift Cluster.
        Arguments are passed from the DAG

        args: {
          : redshift_conn_id = parameters of the redshift connection
          : aws_credentials = AWS credentials
          : sql_statement = SQL statement
          : table = table name
        }
        '''
        self.log.info(f"load table: {self.table}")

        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        redshift.run(self.sql_statement)
