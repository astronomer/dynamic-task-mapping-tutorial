from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import random


@dag(start_date=days_ago(2), schedule=None, catchup=False)
def mapping_example():

    @task
    def make_list():
        return [i + 1 for i in range(random.randint(2, 4))]

    @task
    def consumer(value):
        print(repr(value))

    consumer.expand(value=make_list())


mapping_example()
