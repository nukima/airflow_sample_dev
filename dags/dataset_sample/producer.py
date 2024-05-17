import json
import pendulum
from airflow.decorators import dag, task
from airflow.datasets import Dataset


sample_dataset_1 = Dataset(
    "hdfs:///user/hive/warehouse/cdc/cod_locations_avro_bigdata.cod_locations.check_in_points",
    extra={"team": "Team Secret"},
)
sample_dataset_2 = Dataset(
    "hdfs:///user/hive/warehouse/cdc/cod_locations_avro_bigdata.cod_locations.check_out_points",
    extra={"team": "Team Solo Mid"},    
)

@dag(
    dag_id="datasets_producer",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example', 'dataset'],
)
def dataset_producer():
    @task(outlets=[sample_dataset_1])
    def produce_sample_dataset_1():
        print("Some data is being produced")
        
    @task(outlets=[sample_dataset_2])
    def produce_sample_dataset_2():
        print("Some data is being produced")

    produce_sample_dataset_1()
    produce_sample_dataset_2()

dataset_producer()