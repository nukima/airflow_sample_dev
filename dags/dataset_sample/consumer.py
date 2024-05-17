from pendulum import datetime
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

sample_dataset_1 = Dataset(
    "hdfs:///user/hive/warehouse/cdc/cod_locations_avro_bigdata.cod_locations.check_in_points",
    extra={"team": "Team Secret"},
)
sample_dataset_2 = Dataset(
    "hdfs:///user/hive/warehouse/cdc/cod_locations_avro_bigdata.cod_locations.check_out_points",
    extra={"team": "Team Solo Mid"},    
)


@dag(
    dag_id="datasets_consumer",
    start_date=datetime(2022, 10, 1),
    schedule=[sample_dataset_1, sample_dataset_2],  
    # schedule=(sample_dataset_1 | sample_dataset_2),  
    # schedule=DatasetOrTimeSchedule(
    #     timetable=CronTriggerTimetable("0 1 * * 3", timezone="UTC"), datasets=(sample_dataset_1 & sample_dataset_2)
    # )
    catchup=False,
)
def datasets_consumer_dag():
    @task
    def consume_sample_dataset_1():
        print("Some data is being consumed")

    @task
    def consume_sample_dataset_2():
        print("Some data is being consumed")

    consume_sample_dataset_1()
    consume_sample_dataset_2()



datasets_consumer_dag()