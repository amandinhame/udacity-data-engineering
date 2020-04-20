from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadTableOperator(BaseOperator):
    '''Operator responsible for loading data into table.'''

    ui_color = '#00796B'
    
    delete_sql = 'DELETE FROM {}'
    
    select_insert_sql = '''
        INSERT INTO {}{}
        {}
    '''

    @apply_defaults
    def __init__(self,  redshift_conn_id, table,
                 select_query, clear_table=False,
                 columns=None, *args, **kwargs):
        '''Instantiate LoadTableOperator.
        
        Args:
            redshift_conn_id (str): Redshift connection id registered in Airflow.
            table (str): Table name.
            select_query (str): Query to select data to insert into  table.
            clear_table (bool, optional): Indicates if table should be cleaned before inserting.
            columns ([str], optional): Names of the columns to load. If None is provided, the select query must provide all columns values.
            *args: Variable arguments.
            **kwargs: Keyword arguments.
            
        '''

        super(LoadTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query
        self.clear_table = clear_table
        self.columns = columns
        
    def execute(self, context):
        '''Insert data into table.
        
        Args:
            context: Variable arguments with task information.
            
        '''
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.clear_table:
            self.log.info(f'Clearing data in Redshift table {self.table}')
            delete_query = LoadTableOperator.delete_sql.format(self.table)
            redshift.run(delete_query)
        
        self.log.info(f'Inserting data into {self.table}')
        select_insert_query = LoadTableOperator.select_insert_sql.format(
            self.table,
            '' if self.columns == None else '({})'.format(', '.join(self.columns)),
            self.select_query
        )
        redshift.run(select_insert_query)
        