from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.png import (ProcessS3JsonOperator, StageFromS3ToRedshiftOperator,
                                    StageFromApiToRedshiftOperator, LoadTableOperator,
                                    CheckRowCountOperator, CheckDuplicatesOperator)

from helpers import SqlQueries

def create_dags(name, default_args, args):
    """Creates a dag to run the png data pipeline.

    Args:
        name (str): The name of the dag.
        default_args (dict): The default arguments to be passed to the tasks.
        args (dict): The configuration of the dag.

    Returns:
        dag: The dag object with the tasks associated to it.
    """

    # Dag
    dag = DAG(name,
        default_args=default_args, 
        start_date=args['start_date'],
        schedule_interval=args['schedule_interval'],
        max_active_runs=args['max_active_runs'],
        catchup=args['catchup']
    )

    # Operators

    start_operator = DummyOperator(task_id='begin_execution', dag=dag)

    # Legislatures operators

    process_legislatures = ProcessS3JsonOperator(
        task_id='process_legislatures',
        dag=dag,
        aws_credentials_id='aws_credentials',
        s3_bucket='png-transparency',
        s3_prefix='legislatures/legislatures.json',
        aws_region='us-east-1'
    )

    stage_legislatures = StageFromS3ToRedshiftOperator(
        task_id='staging_legislatures', 
        dag=dag, 
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='stagingLegislatures', 
        s3_bucket='png-transparency',
        s3_prefix='legislatures/legislatures_processed.json',
        aws_region='us-east-1',
        json_format='s3://png-transparency/legislatures/legislatures_path.json', 
        date_format='YYYY-MM-DD'
    )

    load_legislatures = LoadTableOperator(
        task_id='load_legislatures',
        dag=dag,
        redshift_conn_id='redshift',
        table='legislatures',
        select_query=SqlQueries.legislatures_select_insert
    )

    check_rows_legislatures = CheckRowCountOperator(
        task_id='check_rows_legislatures',
        dag=dag,
        redshift_conn_id='redshift',
        staging_table='stagingLegislatures',
        final_table='legislatures',
        exact=True
    )

    # Deputies operators

    process_deputies = ProcessS3JsonOperator(
        task_id='process_deputies',
        dag=dag,
        aws_credentials_id='aws_credentials',
        s3_bucket='png-transparency',
        s3_prefix='deputies/deputies.json',
        aws_region='us-east-1'
    )

    stage_deputies = StageFromS3ToRedshiftOperator(
        task_id='staging_deputies', 
        dag=dag, 
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='stagingDeputies', 
        s3_bucket='png-transparency',
        s3_prefix='deputies/deputies_processed.json',
        aws_region='us-east-1',
        json_format='s3://png-transparency/deputies/deputies_path.json', 
        date_format='YYYY-MM-DD'
    )

    load_new_deputies = LoadTableOperator(
        task_id='load_new_deputies',
        dag=dag,
        redshift_conn_id='redshift',
        table='newDeputies',
        select_query=SqlQueries.new_deputies_select_insert,
        clear_table=True
    )

    stage_deputies_details = StageFromApiToRedshiftOperator(
        task_id='staging_deputies_details',
        dag=dag,
        redshift_conn_id='redshift',
        table='stagingDeputiesDetails',
        select_query=SqlQueries.new_deputies_uri_select,
        # deputyId, deputyDocumentId, party, electionState, schoolLevel
        fields=['dados.id', 'dados.cpf', 'dados.ultimoStatus.siglaPartido', 
                'dados.ultimoStatus.siglaUf', 'dados.escolaridade']
    )

    load_parties = LoadTableOperator(
        task_id='load_parties',
        dag=dag,
        redshift_conn_id='redshift',
        table='parties',
        select_query=SqlQueries.parties_select_insert,
        columns=SqlQueries.parties_columns
    )

    load_states = LoadTableOperator(
        task_id='load_states',
        dag=dag,
        redshift_conn_id='redshift',
        table='states',
        select_query=SqlQueries.states_select_insert,
        columns=SqlQueries.states_columns
    )

    load_school_level = LoadTableOperator(
        task_id='load_school_level',
        dag=dag,
        redshift_conn_id='redshift',
        table='schoolLevels',
        select_query=SqlQueries.school_levels_select_insert,
        columns=SqlQueries.school_levels_columns
    )

    load_deputies = LoadTableOperator(
        task_id='load_deputies',
        dag=dag,
        redshift_conn_id='redshift',
        table='deputies',
        select_query=SqlQueries.deputies_select_insert
    )

    check_rows_deputies = CheckRowCountOperator(
        task_id='check_rows_deputies',
        dag=dag,
        redshift_conn_id='redshift',
        staging_table='stagingDeputies',
        final_table='deputies',
        exact=True
    )

    # Expenses operators

    stage_expenses = StageFromS3ToRedshiftOperator(
        task_id='staging_expenses', 
        dag=dag, 
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='stagingExpenses', 
        s3_bucket='png-transparency',
        s3_prefix='expenses/Year-{}.csv',
        aws_region='us-east-1',
        delimiter=';',
        ignore_header='1',
        remove_quotes=True,
        time_format='YYYY-MM-DDTHH:MI:SS'
    )

    load_expense_types = LoadTableOperator(
        task_id='load_expense_types',
        dag=dag,
        redshift_conn_id='redshift',
        table='expenseTypes',
        select_query=SqlQueries.expense_types_select_insert,
        columns=SqlQueries.expense_types_columns
    )

    load_expenses = LoadTableOperator(
        task_id='load_expenses',
        dag=dag,
        redshift_conn_id='redshift',
        table='expenses',
        select_query=SqlQueries.expenses_select_insert,
        columns=SqlQueries.expenses_columns
    )

    check_rows_expenses = CheckRowCountOperator(
        task_id='check_rows_expenses',
        dag=dag,
        redshift_conn_id='redshift',
        staging_table='stagingExpenses',
        final_table='expenses',
        exact=False
    )

    check_duplicates_expenses = CheckDuplicatesOperator(
        task_id='check_duplicates_expenses',
        dag=dag,
        redshift_conn_id='redshift',
        table='expenses',
        fields=['deputyid', 'documentid', 'documentvalue', 'emissiondate', 'expenseid', 
            'month', 'year', 'providerdocumentid', 'legislatureid', 'parcelnu']
    )

    end_operator = DummyOperator(task_id='stop_execution', dag=dag)

    # Task dependencies

    # Legislatures tasks
    start_operator >> process_legislatures >> stage_legislatures >> load_legislatures 
    load_legislatures >> check_rows_legislatures >> end_operator

    # Deputies tasks
    start_operator >> process_deputies >> stage_deputies >> load_new_deputies >> stage_deputies_details
    stage_deputies_details >> [load_parties, load_states, load_school_level] >> load_deputies 
    load_deputies >> check_rows_deputies >> end_operator

    # Expenses tasks
    start_operator >> stage_expenses >> load_expense_types >> load_expenses 
    load_expenses >> check_rows_expenses >> check_duplicates_expenses >> end_operator
    
    return dag


# Default arguments
default_args = {
    'owner': 'amanda',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_retry': False
}

# Create one dag to backfill the previous years data 
# and another to run once a month to update the current years data.

past = {'task_id': 'png-dag-previous-years',
        'start_date': datetime(2008, 1, 1),
        'end_date': datetime(2019, 1, 1),
        'schedule_interval': '@yearly',
        'max_active_runs': 1,
        'catchup': True}

current = {'task_id': 'png-dag',
        'start_date': datetime(2020, 3, 1),
        'schedule_interval': '@monthly',
        'max_active_runs': 1,
        'catchup': False}

globals()[past['task_id']] = create_dags(past['task_id'], default_args, past)
globals()[current['task_id']] = create_dags(current['task_id'], default_args, current)