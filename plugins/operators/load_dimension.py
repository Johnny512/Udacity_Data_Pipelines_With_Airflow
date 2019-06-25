from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    statement = """
    INSERT INTO {}
    {}
    """
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql=sql

    def execute(self, context):
        self.log.info('LoadDimensionOperator attempting to connect to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('connection successful...')
        try:
            redshift.run("DELETE FROM {}".format(self.table))
        except AirflowException as e:
            self.log.info(e)
        formatted_sql=LoadDimensionOperator.statement.format(
            self.table,
            self.sql
        )
        try:
            redshift.run(formatted_sql)
            self.log.info('Insert successful')
        except AirflowException as e:
            self.log.info(e)
