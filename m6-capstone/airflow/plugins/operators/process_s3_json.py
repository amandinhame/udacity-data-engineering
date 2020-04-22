import json
import re

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ProcessS3JsonOperator(BaseOperator):
    """Pre-process a json file and upload the processed file to Amazon S3.
    This pre-processing is needed since some json files are not adequate to be loaded into Redshift.

    Redshift doesn't support array of objects (objects inside `[]`), just the objects without commas
    between them.

    Also some string fields had some quotes inside and were not being loaded properly into Redshift.
    
    The processed file will be uploaded to S3 in the same location as the original json with a sufix 
    in the file name.

    """
    
    ui_color = '#B2DFDB'
    
    @apply_defaults
    def __init__(self, aws_credentials_id,
                 s3_bucket, s3_prefix, aws_region,
                 sufix='_processed', *args, **kwargs):
        '''Instantiate ProcessS3JsonOperator.
        
        Args:
            aws_credentials_id (str): Aws credentials id registered in Airflow.
            s3_bucket (str): S3 bucket where the json file is stored.
            s3_prefix (str): Prefix of the json file in S3 to be processed.
            aws_region (str): Region of the S3 bucket.
            sufix (str, optional): The sufix to be appended in the processed file name.
            *args: Variable arguments.
            **kwargs: Keyword arguments.
            
        '''

        super(ProcessS3JsonOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_region = aws_region
        self.sufix = sufix
        
    def execute(self, context):
        """Pre-process the json file and upload the processed file to the same location
        of the original file but with a sufix in the file name.
        
        """

        self.log.info(f'Pre-processing file s3://{self.s3_bucket}/{self.s3_prefix}')

        s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)

        s3_obj = s3_hook.get_key(self.s3_prefix, bucket_name=self.s3_bucket)

        # Get S3 file content as json
        file_content = s3_obj.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)

        # Redshift doesn't support array of objects
        json_content_processed = str(json_content['data'])[1:-1]

        # Check quote marks in json string values
        pattern = r":[\s]*['\"](.*?)['\"][\s]*[,}]"
        for m in re.findall(pattern, json_content_processed):
            if len(m) > 0 and ('\'' in m or '\"' in m):
                clean_m = m.replace('\'', '´').replace('\"', '´')
                json_content_processed = json_content_processed.replace(m, clean_m)

        json_content_processed = json_content_processed.replace('\'', '\"')

        # Remove comma between objects
        json_content_processed = re.sub(r'},[\s]*{', '}{', json_content_processed)

        # Upload to S3
        prefix, extension = self.s3_prefix.split('.')
        s3_key = prefix + self.sufix + '.' + extension
        
        self.log.info(f'Uploading processed file s3://{self.s3_bucket}/{s3_key}')
        
        s3_hook.load_string(json_content_processed, s3_key, bucket_name=self.s3_bucket, replace=True)
        