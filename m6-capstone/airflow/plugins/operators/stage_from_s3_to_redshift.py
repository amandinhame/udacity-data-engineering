from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageFromS3ToRedshiftOperator(BaseOperator):
    '''Operator responsible to copy data from S3 to staging tables in Redshift.'''
    
    ui_color = '#4DB6AC'
    
    delete_sql = 'DELETE FROM public.{}'
    
    copy_sql = '''
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {}
        {}
        {}
        {}
        {}
        {}
    '''

    @apply_defaults
    def __init__(self, redshift_conn_id, aws_credentials_id,
                 table, s3_bucket, s3_prefix, aws_region,
                 json_format='', delimiter='', ignore_header='',
                 remove_quotes=False, date_format='', 
                 time_format='', *args, **kwargs):
        '''Instantiate StageFromS3ToRedshiftOperator.
        
        Args:
            redshift_conn_id (str): Redshift connection id registered in Airflow.
            aws_credentials_id (str): Aws credentials id  registered in Airflow.
            table (str): Staging table name.
            s3_bucket (str): S3 bucket where the data is stored.
            s3_prefix (str): Prefix of the files in S3 to be processed.
            aws_region (str): Region of the S3 bucket.
            json_format (str, optional): Format of the json to be processed.
            delimiter (str, optional): Column delimiter in the csv file.
            ignore_header(str, optional): Number of lines to ignore from a csv file.
            remove_quotes(bool, optional): Remove quotes that delimits columns from a csv file.
            date_format (str, optional): Date format of date data in the data files.
            time_format (str, optional): Time format of time data in the data files.
            *args: Variable arguments.
            **kwargs: Keyword arguments.
            
        '''

        super(StageFromS3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_region = aws_region
        self.json_format = json_format
        self.delimiter = delimiter
        self.ignore_header = ignore_header
        self.remove_quotes = remove_quotes
        self.date_format = date_format
        self.time_format = time_format
        
    def execute(self, context):
        '''Clear Redshift table and copy S3 data into it.
        
        Args:
            context: Variable arguments with task information.
            
        '''
        print(context)
        aws_hook = AwsHook(aws_conn_id=self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Clearing data in Redshift table {self.table}')
        delete_query = StageFromS3ToRedshiftOperator.delete_sql.format(self.table)
        redshift.run(delete_query)

        s3_path = 's3://{}/{}'.format(self.s3_bucket, self.s3_prefix.format(context['execution_date'].year))
        
        self.log.info(f'Copying data from {s3_path} to Redshift table {self.table}')
        copy_query = StageFromS3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.aws_region,
            '' if len(self.json_format) == 0 else f'FORMAT AS JSON \'{self.json_format}\'',
            '' if len(self.delimiter) == 0 else f'DELIMITER \'{self.delimiter}\'',
            '' if len(self.ignore_header) == 0 else f'IGNOREHEADER {self.ignore_header}',
            '' if not self.remove_quotes else 'REMOVEQUOTES',
            '' if len(self.date_format) == 0 else f'DATEFORMAT AS \'{self.date_format}\'',
            '' if len(self.time_format) == 0 else f'TIMEFORMAT AS \'{self.time_format}\''
        )
        redshift.run(copy_query)
        