from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    statement="""
        SELECT COUNT(1) AS count
        FROM {}
        WHERE {} IS NULL
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 tables={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables
    
    def runSql(self,x):
        myObj=self.tables
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('connection to Redshift successful...')
        formatted_sql=DataQualityOperator.statement.format(myObj["tables"][x], myObj["fields"][x])
        num=redshift.get_first(formatted_sql)
        num=num[0]
        return num
    
    def execute(self,context):
        myObj=self.tables
        self.log.info('DataQualityOperator is running...')
        for i in range(4):
            results=0
            results=self.runSql(i)
            if results==0:
                self.log.info(f'Table {myObj["tables"][i]} passes data quality, running next query')
                continue
                
            else:
                raise ValueError(f'Table {myObj["tables"][i]} has nulls in {myObj["fields"][i]}')
                continue