from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql="""
        COPY {}
        FROM {}
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        json '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 region = "",
                 file_format = "JSON",
                 json_path = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        '''
        This Operator copy JSON data from a AWS S3 storage to a AWS Redshift Cluster.
        Arguments are passed from the DAG

        args: {
          : redshift_conn_id = parameters of the redshift connection
          : aws_credentials = AWS credentials
          : table = table name in the Redshift Cluster
          : s3_bucket = S3 bucket file_path
          : s3_key = subfolder from the S3 file_path
          : region = the region of the AWS Redshift Cluster
          : file_format = JSON or CSV
          : execution_date = passed from the kwargs
        }
        '''
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        rend_key = self.s3_key.format(**context)
        s3_path = "'s3://{}/{}'".format(self.s3_bucket, rend_key)
        form_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        redshift.run(form_sql)
