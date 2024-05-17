"""Example DAG demonstrating the usage of dynamic task mapping."""
import pendulum
from datetime import datetime

from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    dag_id="example_dynamic_task_mapping",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example', 'dataset'],
)
def example_dynamic_task_mapping():
    #basic example
    @task
    def make_list():
        # This can also be from an API call, checking a database, -- almost anything you like, as long as the
        # resulting list/dictionary can be stored in the current XCom backend.
        return [1, 2, {"a": "b"}, "str"]

    @task(max_active_tis_per_dagrun=2)
    def consumer(arg):
        print(arg)

    consumer.expand(arg=make_list())
    # SQLExecuteQueryOperator.partial(
    #     task_id="fetch_data",
    #     sql="SELECT * FROM data WHERE date = %(date)s",
    #     map_index_template="""{{ task.parameters['date'] }}""",
    # ).expand(
    #     parameters=[{"date": "2024-01-01"}, {"date": "2024-01-02"}],
    # )

    #dynamically inject into the rendering context

example_dynamic_task_mapping()