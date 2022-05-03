from airflow import DAG
from airflow.utils.dates import days_ago

import random

with DAG(dag_id='mapping_example', start_date=days_ago(2), catchup=False) as dag:

    @dag.task
    def make_list():
        return [i + 1 for i in range(random.randint(2, 4))]

    @dag.task
    def consumer(value):
        print(repr(value))

    consumer.expand(value=make_list())