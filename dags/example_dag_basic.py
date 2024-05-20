import json
import pendulum
from airflow.decorators import dag, task

default_args = {
    'owner': 'manhnk9'
}

@dag(
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
    max_consecutive_failed_dag_runs=3,
)
def tutorial_taskflow_api_etl():
    @task()
    def extract():
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict
    
    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}
    
    @task()
    def load(total_order_value: float):
        print(f"Total order value is: {total_order_value:.2f}")

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])

    
tutorial_etl_dag = tutorial_taskflow_api_etl()
