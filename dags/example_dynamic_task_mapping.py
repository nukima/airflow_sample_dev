"""Example DAG demonstrating the usage of dynamic task mapping."""
import pendulum
from datetime import datetime

from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.baseoperator import BaseOperator


class AddOneOperator(BaseOperator):
    """A custom operator that adds one to the input."""

    def __init__(self, value, **kwargs):
        super().__init__(**kwargs)
        self.value = value

    def execute(self, context):
        return self.value + 1
    
class SumItOperator(BaseOperator):
    """A custom operator that sums the input."""

    template_fields = ("values",)

    def __init__(self, values, **kwargs):
        super().__init__(**kwargs)
        self.values = values

    def execute(self, context):
        total = sum(self.values)
        print(f"Total was {total}")
        return total

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

    @task(max_active_tis_per_dagrun=1)
    def consumer(arg1, arg2):
        import time
        print(arg1)
        print(arg2)
        time.sleep(5)

    consumer.partial(arg2='constant').expand(arg1=make_list())

    add_one_task = AddOneOperator.partial(task_id="add_one").expand(value=[1, 2, 3])
    sum_it_task = SumItOperator(task_id="sum_it", values=add_one_task.output)
    
    # SQLExecuteQueryOperator.partial(
    #     task_id="fetch_data",
    #     sql="SELECT * FROM data WHERE date = %(date)s",
    #     map_index_template="""{{ task.parameters['date'] }}""",
    # ).expand(
    #     parameters=[{"date": "2024-01-01"}, {"date": "2024-01-02"}],
    # )

    #dynamically inject into the rendering context

example_dynamic_task_mapping()