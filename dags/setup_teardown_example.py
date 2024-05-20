import json
import pendulum
from airflow.decorators import dag, task, setup, teardown

default_args = {
    'owner': 'manhnk9'
}

@dag(
    dag_id="setup_teardown_example",
    default_args=default_args,
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
    max_consecutive_failed_dag_runs=3,
)
def setup_teardown_example():
    @setup
    def my_cluster_setup_task():
        print("Setting up resources!")
        my_cluster_id = "cluster-2319"
        return my_cluster_id

    @task
    def my_cluster_worker_task():
        return "Doing some work!"

    @teardown
    def my_cluster_teardown_task(my_cluster_id):
        return f"Tearing down {my_cluster_id}!"

    @setup
    def my_database_setup_task():
        print("Setting up my database!")
        my_database_name = "DWH"
        return my_database_name

    @task
    def my_database_worker_task():
        return "Doing some work!"

    @teardown
    def my_database_teardown_task(my_database_name):
        return f"Tearing down {my_database_name}!"

    my_setup_task_obj = my_cluster_setup_task()
    (
        my_setup_task_obj
        >> my_cluster_worker_task()
        >> my_cluster_teardown_task(my_setup_task_obj)
    )

    my_database_setup_obj = my_database_setup_task()
    (
        my_database_setup_obj
        >> my_database_worker_task()
        >> my_database_teardown_task(my_database_setup_obj)
    )

setup_teardown_example()