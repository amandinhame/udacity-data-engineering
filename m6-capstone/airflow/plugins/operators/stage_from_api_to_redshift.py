import requests
import json

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageFromApiToRedshiftOperator(BaseOperator):
    '''Operator responsible for loading data into table from a api request.'''

    ui_color = '#009688'
    
    delete_sql = 'DELETE FROM {}'
    
    insert_sql = '''
        INSERT INTO {}
        VALUES {}
    '''

    @apply_defaults
    def __init__(self,  redshift_conn_id, table,
                 select_query, fields,
                 *args, **kwargs):
        '''Instantiate StageFromApiToRedshiftOperator.
        
        Args:
            redshift_conn_id (str): Redshift connection id registered in Airflow.
            table (str): Table to insert the data from the API response.
            select_query (str): Select query that returns only one column with the urls to be requested.
            fields (arr[str]): Fields to be obtained from the api response. E.g.: ['data.id', 'data.name'].
            *args: Variable arguments.
            **kwargs: Keyword arguments.
            
        '''

        super(StageFromApiToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query
        self.fields = fields
        
    def execute(self, context):
        '''Obtain the urls to request data from the API, get data from the response and insert into a table.
        
        Args:
            context: Variable arguments with task information.
            
        '''
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Clearing data in Redshift table {self.table}')
        delete_query = StageFromApiToRedshiftOperator.delete_sql.format(self.table)
        redshift.run(delete_query)

        conn = redshift.get_conn() 
        cursor = conn.cursor()

        # This ´select´ only returns the url to be requested
        self.log.info('Select uris to request more data')
        cursor.execute(self.select_query)
        rows = cursor.fetchall()

        data = []
        self.log.info(f'Requesting and inserting data into {self.table}')
        for row in rows:

            # Request data and transform to json dict
            r = requests.get(row[0])
            content = json.loads(r.text)

            line = []
            for field in self.fields:
                # Parse fields: from abc.def to content['abc']['def']
                cmd = 'content[\'{}\']'.format('\'][\''.join(field.split('.')))
                value = str(eval(cmd))
                line.append('' if value == 'None' else value)

            # Append to ´data´ each line of the insert: 'field1', 'field2', ..., 'fieldn'
            data.append('\'{}\''.format('\', \''.join(line)))

        # Build the ´values´ part of the insert statement: ('field1', 'field2', ..., 'fieldn'), ..., ('field1', 'field2', ..., 'fieldn')
        values = '({})'.format('), ('.join(data))

        # Insert data into table
        if len(data) > 0:
            insert_statement = StageFromApiToRedshiftOperator.insert_sql.format(self.table, values)
            redshift.run(insert_statement)

        self.log.info(f'Request data from API {self.table}')
        
        cursor.close()
        conn.close()