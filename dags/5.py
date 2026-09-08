from airflow.sdk import dag,task,asset
from pendulum import datetime
import os 
from fetch import fetch

@asset(
        schedule=fetch,
        uri= "/opt/airflow/logs/data/processed.txt",
        name="process"
)
def fetch(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    with open(self.uri,'w') as f:
        f.write('data fetched')
    print("data written")

