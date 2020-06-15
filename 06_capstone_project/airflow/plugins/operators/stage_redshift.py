from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql_parquet = """
    COPY {}
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS PARQUET;
    """
    copy_sql_csv = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION 'us-east-1' csv;
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 format="",
                 source_file="",
                 ignore_headers=1,
                 aws_credentials_id="",
                 iam_role="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.format = format
        self.source_file = source_file
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.iam_role = iam_role

    def get_formatted_sql(self):

        if self.format == "parquet":
            formatted_sql = StageToRedshiftOperator.copy_sql_parquet.format(
                self.table,
                self.s3_path,
                self.iam_role
            )
        elif self.format == "csv":
            formatted_sql = StageToRedshiftOperator.copy_sql_csv.format(
                self.table,
                self.s3_path,
                self.credentials.access_key,
                self.credentials.secret_key
            )
        else:
            raise Exception("Format not suported.")
        
        return formatted_sql

    def execute(self, context):

        aws_hook = S3Hook(aws_conn_id=self.aws_credentials_id, verify=False)
        self.credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {};".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        path = self.s3_key + self.source_file
        rendered_key = path.format(**context)
        self.s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_sql = self.get_formatted_sql()

        redshift.run(formatted_sql)