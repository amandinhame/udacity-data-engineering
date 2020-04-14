from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    '''Operator responsible to copy data from S3 to staging tables in Redshift.'''
    
    ui_color = '#358140'
    
    delete_sql = 'DELETE FROM public.{}'
    
    copy_sql = '''
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {}
        {}
    '''

    @apply_defaults
    def __init__(self, redshift_conn_id, aws_credentials_id,
                 table, s3_bucket, s3_prefix, aws_region,
                 json_format='', timeformat='', *args, **kwargs):
        '''Instantiate StageToRedshiftOperator.
        
        Args:
            redshift_conn_id (str): Redshift connection id registered in Airflow.
            aws_credentials_id (str): Aws credentials id  registered in Airflow.
            table (str): Staging table name.
            s3_bucket (str): S3 bucket where the data is stored.
            s3_prefix (str): Prefix of the files in S3 to be processed.
            aws_region (str): Region of the S3 bucket.
            json_format (str, optional): Format of the json to be processed.
            timeformat (str, optional): Time format of time data in the data files.
            *args: Variable arguments.
            **kwargs: Keyword arguments.
            
        '''

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_region = aws_region
        self.json_format = json_format
        self.timeformat = timeformat
        
    def execute(self, context):
        '''Clear Redshift table and copy S3 data into it.
        
        Args:
            context: Variable arguments with task information.
            
        '''
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Clearing data in Redshift table {self.table}')
        delete_query = StageToRedshiftOperator.delete_sql.format(self.table)
        redshift.run(delete_query)

        s3_path = 's3://{}/{}'.format(self.s3_bucket, self.s3_prefix).format(**context)
        
        self.log.info(f'Copying data from {s3_path} to Redshift table {self.table}')
        copy_query = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.aws_region,
            '' if len(self.json_format) == 0 else f'format as json \'{self.json_format}\'',
            '' if len(self.timeformat) == 0 else f'timeformat as \'{self.timeformat}\''
        )
        redshift.run(copy_query)
        





