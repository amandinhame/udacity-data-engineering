from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CheckDuplicatesOperator(BaseOperator):
    '''Operator responsible to check if there are duplicated rows.'''

    ui_color = '#004D40'
    
    select_duplicates_sql = '''
        SELECT COUNT(*)
        FROM (
        SELECT {}, COUNT(*) AS dup
        FROM {}
        GROUP BY {}
        )
        WHERE dup > 1
    '''

    @apply_defaults
    def __init__(self,  redshift_conn_id, table,
                 fields, *args, **kwargs):
        '''Instantiate CheckDuplicatesOperator.
        
        Args:
            redshift_conn_id (str): Redshift connection id registered in Airflow.
            table (str): Table name.
            fields (arr): Name of the columns of `table` that if two or more rows have the same values
                          they are considered duplicates.
            *args: Variable arguments.
            **kwargs: Keyword arguments.
            
        '''

        super(CheckDuplicatesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.fields = fields
        
    def execute(self, context):
        '''
        Check if there are duplicated rows in a table.

        Raise a `ValueError` exception if two or more rows are duplicated.

        Args:
            context: Variable arguments with task information.

        '''
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Checking table {self.table}')
        fields = ', '.join(self.fields)
        rows = redshift.get_records(CheckDuplicatesOperator.select_duplicates_sql.format(fields, self.table, fields))
        
        if len(rows) < 1 or len(rows[0]) < 1:
            raise ValueError(f'Duplicate rows check failed: {self.table} returned no results')

        dup_count = rows[0][0]

        if dup_count > 0:
            raise ValueError(f'Duplicate rows check failed: {self.table} with {dup_count} duplicated rows')

        self.log.info(f'Duplicate rows check passed')
