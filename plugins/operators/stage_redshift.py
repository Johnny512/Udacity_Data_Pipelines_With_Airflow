from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {} 'auto'
    """
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 data_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.data_format=data_format

    def execute(self, context):
        self.log.info('StageToRedshiftOperator is starting...')
        try:
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            self.log.info('Got the creds!')
        except AirflowException as e:
            self.log.info('Cannot retrieve s3 credentials')
            
        self.log.info('Attempting connection to redshift...')
        try:
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            self.log.info('connection successful...')
        except AirflowException as e:
            self.log.info(e)
        
        self.log.info("Clearing data from destination Redshift table")
        try:
            redshift.run("DELETE FROM {}".format(self.table))
        except AirflowException as e:
            self.log.info(e)
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.data_format 
        )
        try:
            redshift.run(formatted_sql)
            self.log.info('copy successful...')
        except AirflowException as e:
            self.log.info(e)
        





