from airflow.plugins_manager import AirflowPlugin

from operators.process_s3_json import *
from operators.stage_from_s3_to_redshift import *
from operators.stage_from_api_to_redshift import *
from operators.load_table import *
from operators.check_row_count import *
from operators.check_duplicates import *

from helpers.sql_queries import *


class PngPlugin(AirflowPlugin):

    name = 'png'

    operators = [
        ProcessS3JsonOperator,
        StageFromS3ToRedshiftOperator,
        StageFromApiToRedshiftOperator,
        LoadTableOperator,
        CheckRowCountOperator,
        CheckDuplicatesOperator
    ]

    helpers = [
        SqlQueries
    ]
