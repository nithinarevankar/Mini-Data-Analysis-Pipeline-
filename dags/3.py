from airflow.sdk import dag,task, get_current_context
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime

@dag(
    dag_id="67",
    schedule=CronDataIntervalTimetable("@daily",timezone='Asia/Kolkata'),
    start_date=datetime(year=2026,month=9,day=1,tz='Asia/Kolkata'),
    end_date=datetime(year=2026,month=9,day=3,tz='Asia/Kolkata'),
    catchup=True


)
def load():
    @task.python
    def extract():
        from airflow.sdk import get_current_context
        context = get_current_context()
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]
        print(f"fetch date {data_interval_start} to {data_interval_end}")
    @task.bash
    def process():
        return "echo 'process {{ data_interval_start }} to {{ data_interval_end }}'"

    extract() >> process()

load()