from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CheckRowCountOperator(BaseOperator):
    '''Operator responsible to check if there is data in the staging and final tables and 
    if the number of rows in the final table is equal (exact param) or greater than the final table.
    
    '''

    ui_color = '#004D40'
    
    select_table_count_sql = '''
        SELECT COUNT(*) AS rowCount FROM {}
    '''

    @apply_defaults
    def __init__(self,  redshift_conn_id, staging_table,
                 final_table, exact=False, *args, **kwargs):
        '''Instantiate CheckRowCountOperator.
        
        Args:
            redshift_conn_id (str): Redshift connection id registered in Airflow.
            staging_table (str): Staging table name.
            final_table (str): Final table name.
            exact (bool, optional): Staging and final table need to have exact number of rows. 
                                    If False, final table can have more rows than staging table.
            *args: Variable arguments.
            **kwargs: Keyword arguments.
            
        '''

        super(CheckRowCountOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.staging_table = staging_table
        self.final_table = final_table
        self.exact = exact
        
    def execute(self, context):
        '''
        Check the number of rows in the staging and final tables.

        Args:
            context: Variable arguments with task information.
            
        '''
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Checking table {self.staging_table}')
        staging_rows = redshift.get_records(CheckRowCountOperator.select_table_count_sql.format(self.staging_table))
        
        if len(staging_rows) < 1 or len(staging_rows[0]) < 1:
            raise ValueError(f'Row count check failed: {self.staging_table} returned no results')

        self.log.info(f'Checking table {self.final_table}.')
        final_rows = redshift.get_records(CheckRowCountOperator.select_table_count_sql.format(self.final_table))
        
        if len(final_rows) < 1 or len(final_rows[0]) < 1:
            raise ValueError(f'Row count check failed: {self.final_table} returned no results')

        staging_count = staging_rows[0][0]
        final_count = final_rows[0][0]

        if staging_count < 1 or final_count < 1 or (self.exact and staging_count != final_count) or final_count < staging_count:
            raise ValueError(f'Row count check failed: {self.staging_table} with {staging_count} rows and {self.final_table} with {final_count} rows')

        self.log.info(f'Row count check passed: {self.staging_table} with {staging_count} rows and  {self.final_table} with {final_count} rows')
