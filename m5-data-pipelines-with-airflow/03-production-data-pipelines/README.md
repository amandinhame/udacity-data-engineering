# Production Data Pipelines

## Extanding Airflow

Custom operators and hooks can be found in [https://github.com/apache/airflow/tree/master/airflow/contrib]

## Task Boundaries

DAGs should be defined such that they are:
* Atomic and have a single purpose
* Maximize parallelism
* Make failure states obvious

## SubDAGs

Commonly repeated series of tasks within DAGs can be captured as reusable SubDAGs. They are useful, but can limit the visibility of the tasks in Airflow, more difficult to understand.

## Monitoring

Airflow can be configured to email a list of DAGs with missed SLAs. Failed emails can be used to trigger alerts.
It's possbile to monitor Airflow metrics using statsd and Grafana.