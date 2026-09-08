from airflow.sdk import dag,task,asset
from pendulum import datetime
import os 

@asset(
        schedule="@daily",
        uri= "/opt/airflow/logs/data/data.txt",
        name="fetch"
)
def fetch(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    with open(self.uri,'w') as f:
        f.write('data fetched')
    print("data written")

