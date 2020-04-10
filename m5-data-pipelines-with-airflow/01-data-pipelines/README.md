# Data Pipelines

## What is Data Pipeline

A series of steps in which data is processed.

The execution of data pipelines can be scheduled or triggered by events.

## Data Validation

The process of ensuring that data is present, correct and meaningful.

## DAGs

Are Directed Acyclic Graphs and can be used to represent a data pipeline, where nodes are the steps and the edges are the dependencies or relationship between them.

## Apache Airflow

It consists of:

* Scheduler: orchestrate the execution of jobs.
* Work Queue: holds the state of running DAGs and tasks.
* Worker Processes: execute the operations in each DAG.
* Database: saves credentials, connections, history and configuration.
* Web Interface: dashboard for users and maintainers.

Example:

```
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    print('Hello World')

divvy_dag = DAG('divvy',
    description='Analyzes Divvy Bikeshare Data',
    start_date=datetime(2019, 2, 4),
    schedule_interval='@daily')

task = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world,
    dag=divvy_dag)
```

The tasks can be run:

@once - Run a DAG once and then never again
@hourly - Run the DAG every hour
@daily - Run the DAG every day (default)
@weekly - Run the DAG every week
@monthly - Run the DAG every month
@yearly- Run the DAG every year
None - Only run the DAG when the user initiates it

The order of the tasks can be set using >> or <<:

```
hello_world_task = PythonOperator(task_id=’hello_world’, ...)
goodbye_world_task = PythonOperator(task_id=’goodbye_world’, ...)
...
# Use >> to denote that goodbye_world_task depends on hello_world_task
hello_world_task >> goodbye_world_task
```

## Hooks

Connections can be accessed by hooks, which provides a reusable interface to external systems and databases.

```
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

def load():
# Create a PostgresHook option using the `demo` connection
    db_hook = PostgresHook(‘demo’)
    df = db_hook.get_pandas_df('SELECT * FROM rides')
    print(f'Successfully used PostgresHook to return {len(df)} records')

load_task = PythonOperator(task_id=’load’, python_callable=hello_world, ...)
```

## Runtime variables

[Airflow macro reference](https://airflow.apache.org/docs/stable/macros-ref.html)

```
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_date(*args, **kwargs):
    print(f"Hello {kwargs['execution_date']}")

divvy_dag = DAG(...)
task = PythonOperator(
    task_id='hello_date',
    python_callable=hello_date,
    provide_context=True,
    dag=divvy_dag)
```