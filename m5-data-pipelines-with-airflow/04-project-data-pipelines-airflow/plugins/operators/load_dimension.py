from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''Operator responsible for loading dimension table from staging tables data.'''

    ui_color = '#80BD9E'
    
    delete_sql = 'DELETE FROM {}'
    
    select_insert_sql = '''
        INSERT INTO {}
        {}
    '''

    @apply_defaults
    def __init__(self,  redshift_conn_id, table,
                 select_query, clear_table=False,
                 *args, **kwargs):
        '''Instantiate LoadDimensionOperator.
        
        Args:
            redshift_conn_id (str): Redshift connection id registered in Airflow.
            table (str): Dimension table name.
            select_query (str): Query to select data from staging tables to insert into dimension table.
            clear_table (bool, optional): Indicates if dimension table should be cleaned before inserting.
            *args: Variable arguments.
            **kwargs: Keyword arguments.
            
        '''

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query
        self.clear_table = clear_table
        
    def execute(self, context):
        '''Insert data from staging tables to dimension table.
        
        Args:
            context: Variable arguments with task information.
            
        '''
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.clear_table:
            self.log.info(f'Clearing data in Redshift table {self.table}')
            delete_query = LoadDimensionOperator.delete_sql.format(self.table)
            redshift.run(delete_query)
        
        self.log.info(f'Inserting data into {self.table}')
        select_insert_query = LoadDimensionOperator.select_insert_sql.format(
            self.table,
            self.select_query
        )
        redshift.run(select_insert_query)
        
