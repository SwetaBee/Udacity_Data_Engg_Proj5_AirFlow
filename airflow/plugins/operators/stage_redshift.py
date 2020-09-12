from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    """Transfer data from S3 to staging tables in redshift database.
    
    Parameters:
    aws_credentials_id: Conn Id of the Airflow connection to Amazon Web Services
    redshift_conn_id: Conn Id of the Airflow connection to redshift database
    table: name of the staging table to populate
    s3_bucket: name of S3 bucket, e.g. "udacity-dend"
    s3_key: name of S3 key. This field is templatable when context is enabled, e.g. "log_data/{execution_date.year}/{execution_date.month}/"
    delimiter: csv field delimiter
    ignore_headers: '0' or '1'
    data_format: 'csv' or 'json'
    jsonpaths: path to JSONpaths file
    
    Returns: None
    """
    ui_color = '#8EB6D4'

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_prefix,
                 table,
                 redshift_conn_id='redshift',
                 aws_conn_id='aws_credentials',
                 copy_options='',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options

    def execute(self, context):
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")

        self.log.info(f'Preparing to stage data from {self.s3_bucket}/{self.s3_prefix} to {self.table} table...')

        copy_query = """
                    COPY {table}
                    FROM 's3://{s3_bucket}/{s3_prefix}'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {copy_options};
                """.format(table=self.table,
                           s3_bucket=self.s3_bucket,
                           s3_prefix=self.s3_prefix,
                           access_key=credentials.access_key,
                           secret_key=credentials.secret_key,
                           copy_options=self.copy_options)

        self.log.info('Executing COPY command...')
        redshift_hook.run(copy_query)
        self.log.info("COPY command complete.")
