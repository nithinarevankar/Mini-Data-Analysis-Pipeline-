import os
import glob
from datetime import datetime, timezone, date
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO,format="%(asctime)s[%(levelname)s]%(message)s")
log = logging.getLogger(__name__)


DATA_DB_CONN = os.getenv("DB_CONN_ENV")
SLIVER_SCHEME = "silver"
BRONZE_SCHEMA = "bronze"

SOURCE_TABLE = [
    'sales'
]

def engine_creation():
    data_engine = create_engine(DATA_DB_CONN)
    return data_engine

def ensure_silver_schema(data_engine):
    with data_engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SLIVER_SCHEME}"))

def metadata_insert(data_engine,size,watermark):
    query = f"""INSERT INTO {SLIVER_SCHEME}.metadata(row_counting,watermark) VALUES(
    :s ,:wm
    )"""
    with data_engine.begin() as conn:
            conn.execute(text(query),{"s":size,"wm":watermark})

def extract(source_engine,last_time_loaded,source_table: str):
     query = text(f"SELECT * FROM {BRONZE_SCHEMA}.{source_table} WHERE loaded_time > :ts")
     df = pd.read_sql(query,source_engine,params={"ts":last_time_loaded})
     return df 

def load(data_engine,df,source_table):
    size = len(df)
    time = datetime.now(timezone.utc)
    df['loaded_time']= time
    log.info(len(df))
    df.to_sql(
                source_table,
                data_engine,
                schema=SLIVER_SCHEME,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )
    return size,time
def get_last_watermark(data_engine):
     query = f"""SELECT watermark FROM silver.metadata ORDER BY watermark DESC LIMIT 1"""
     with data_engine.begin() as conn:
            last_time_loaded = conn.execute(text(query)).scalar()
     return last_time_loaded

def cleaning(data):
    data = data.copy()
    for col in data.columns:
       
        null_per = data[col].isnull().mean() * 100
        if null_per < 5:
            data.dropna(subset=[col], inplace=True)
           
        else:
            if data[col].dtype in ['int64', 'float64']:
                data[col] = data[col].fillna(data[col].median())
                
            else:
                data[col] = data[col].fillna(data[col].mode()[0])
    log.info(len(data))
    return data

def eda_report(df):

    summary = pd.DataFrame({
        "column": df.columns,
        "dtype": df.dtypes.values,
        "missing_count": df.isnull().sum().values,
        "missing_percentage": (df.isnull().mean() * 100).values,
        "unique_values": df.nunique().values
    }
        )
    return summary.to_string(index=False)

def main():
     data_engine = engine_creation()
     ensure_silver_schema(data_engine)
     try:
        for source_table in SOURCE_TABLE:
             last = get_last_watermark(data_engine)
             data = extract(data_engine,last,source_table)
             if data.empty:
                  print("no new data")
             else:
                  print(f"data before cleaning \n{eda_report(data)}")
                  data=cleaning(data)
                  print(f"data after cleaning \n{eda_report(data)}")
                  size,time =load(data_engine,data,source_table=source_table)
                  metadata_insert(data_engine,size=size,watermark=time)
     except Exception as e:
          print("error")
          raise

if __name__ == "__main__":
     main()
     

               



