from airflow.sdk import dag,task
from pendulum import datetime
@dag(
    dag_id="1",
    dag_display_name="89r",
    start_date=datetime(year=2026,month=9,day=5,tz="Asia/Kolkata"),
    schedule="@daily",
    catchup=True
)
def workflows1():
    @task.python
    def process_1(**kwargs):
        print("extracting")
        fetched = {"data":[1,2,3,6],"id":[2,3],"s3":True}
        ti = kwargs['ti']
        ti.xcom_push(key='fetch',value=fetched)
        
    @task.python
    def process_2(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids ='process_1',key ='fetch')
        data_2 = data['data']
        trans = [x*2 for x in data_2]
        ti.xcom_push(key='t1',value=trans)

    @task.python
    def process_3(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids ='process_1',key ='fetch')
        data_2 = data['id']
        trans = [x*2 for x in data_2]
        ti.xcom_push(key='t2',value=trans)   

    @task.branch
    def decide(**kwargs):
        ti = kwargs['ti']
        flag = ti.xcom_pull(task_ids = 'process_1', key ='fetch')
        flag = flag['s3']
        if flag == True:
            return 'process_4'
        else :
            return 'no_load_task'
    @task.python
    def process_4(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids ='process_2',key='t1')
        data2 = ti.xcom_pull(task_ids ='process_3',key='t2')
        print(data,data2)

    @task.bash
    def no_load_task(**kwargs):
        return "echo false"


    r1 = process_1()
    r2 = process_2()
    r3 = process_3()
    r4 = process_4()
    r5 = no_load_task()

    r1 >> [r2,r3] >> decide() >> [r4,r5]

workflows1()