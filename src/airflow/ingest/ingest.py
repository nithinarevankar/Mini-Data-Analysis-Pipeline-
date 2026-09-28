import os
import glob
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO,format="%(asctime)s[%(levelname)s]%(message)s")
log = logging.getLogger(__name__)

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

SOURCE_DB_CONN = os.getenv('DB_SOURCE_CONN_ENV')
DATA_DB_CONN = os.getenv('DB_CONN_ENV')

BRONZE_SCHEMA = "bronze"
SOURCE_TABLE =[
    "sales.sales"
]

def engine_creation():
    source_engine = create_engine(SOURCE_DB_CONN)
    data_engine = create_engine(DATA_DB_CONN)
    return source_engine,data_engine

def ensure_bronze_schema(data_engine):
    with data_engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}"))

def bronze_table_name(source_table_name: str) ->str:
    return source_table_name.split('.')[-1]

def metadata_insert(data_engine,size,watermark):
    query = f"""INSERT INTO {BRONZE_SCHEMA}.metadata(row_counting,watermark) VALUES(
    :s ,:wm
    )"""
    with data_engine.begin() as conn:
            conn.execute(text(query),{"s":size,"wm":watermark})

def get_last_watermark(data_engine):
     query = f"""SELECT watermark FROM bronze.metadata ORDER BY watermark DESC LIMIT 1"""
     with data_engine.begin() as conn:
            last_time_loaded = conn.execute(text(query)).scalar()
     return last_time_loaded

def extract(source_engine,last_time_loaded,source_table: str):
     query = text(f"SELECT * FROM {source_table} WHERE source_loaded_time > :ts")
     df = pd.read_sql(query,source_engine,params={"ts":last_time_loaded})
     return df 

def load(data_engine,df,source_table):
    size = len(df)
    time = datetime.now(timezone.utc)
    df['loaded_time']= time
    target = bronze_table_name(source_table)
    df.to_sql(
                target,
                data_engine,
                schema=BRONZE_SCHEMA,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )
    return size,time

def main():
     source_engine,data_engine = engine_creation()
     try:
        for source_tabel in SOURCE_TABLE:
         ensure_bronze_schema(data_engine)
         last_time_loaded = get_last_watermark(data_engine)
         data= extract(source_engine,last_time_loaded,source_tabel)
         if data.empty:
             print("no new data")
         else:
            size,time = load(data_engine,data,source_tabel)
            metadata_insert(data_engine,size,time)
     except Exception as e:
         print("error")
         raise

if __name__ =="__main__":
    main()