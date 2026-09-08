from airflow.sdk import dag,task
from pendulum import datetime, duration 
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
    dag_id="dag_1", 
    start_date=datetime(year=2026,month=9,day=1,tz="Asia/Kolkata"),
    end_date=datetime(year=2026,month=9,day=10,tz="Asia/Kolkata"),
    catchup=True,
    schedule= DeltaTriggerTimetable(duration(days=2))
    
)
def workflow():
    @task.python
    def task_1(**kwargs):
        ti = kwargs['ti']
        data = [1,2,3]
        flag = True
        ti.xcom_push(key='fetch',value=data)
        ti.xcom_push(key='flag_push',value=flag)
    @task.branch
    def decide(**kwargs):
        ti = kwargs['ti']
        flag = ti.xcom_pull(task_ids='task_1',key='flag_push')
        data = ti.xcom_pull(task_ids='task_1',key='fetch')
        if len(data) <= 3 and flag == True:
            return 'multiple'
        else:
            return 'adds'

    @task.python
    def multiple(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='task_1',key='fetch')
        data_2 = [x*2 for x in data]
        print(data_2)
    @task.python
    def adds(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='task_1',key='fetch')
        data_2 = [x+2 for x in data]
        print(data_2)

    r1 = task_1()
    r2=adds()
    r3=multiple()

    r1 >> decide() >> [r2,r3]

workflow()