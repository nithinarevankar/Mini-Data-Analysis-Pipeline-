from airflow.sdk import dag,task
from datetime import datetime,timedelta
from src.airflow.ingest.ingest import main as ingest_main
from src.airflow.transformation.silver import main as silver_tranform
from src.airflow.transformation.gold import main as gold_transform


@dag(
    # schedule="@daily",
    dag_id="data_pipeline",
    # start_date=datetime(2026,9,27),
    # catchup=False,
    default_args={
        "retries":2,
        "retry_delay":timedelta(seconds=10)
    },

)
def retail_sales():
    @task.python
    def load_data():
        ingest_main()
    @task.python
    def silver_load():
        silver_tranform()
    @task.python
    def gold_load():
        gold_transform()
        

    bronze = load_data()
    silver = silver_load()
    gold = gold_load()

    bronze >> silver >> gold

retail_sales()