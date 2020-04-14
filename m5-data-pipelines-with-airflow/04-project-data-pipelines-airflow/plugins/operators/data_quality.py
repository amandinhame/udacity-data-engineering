from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''Operator responsible for data quality check.'''

    ui_color = '#89DA59'
    
    count_sql = 'SELECT COUNT(1) FROM public.{}'

    @apply_defaults
    def __init__(self, redshift_conn_id, tables, 
                 *args, **kwargs):
        '''Instantiate DataQualityOperator.
        
        Args:
            redshift_conn_id (str): Redshift connection id registered in Airflow.
            tables (str[]): Array of table names.
            *args: Variable arguments.
            **kwargs: Keyword arguments.
            
        '''

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        '''Count the rows of tables to check if they are not empty.
        
        Args:
            context: Variable arguments with task information.
            
        '''
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info(f'Checking table {table}')
            records = redshift.get_records(DataQualityOperator.count_sql.format(table))
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data quality check failed. {table} returned no results')
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f'Data quality check failed. {table} contained 0 rows')
            self.log.info(f'Data quality on table {table} check passed with {records[0][0]} records')