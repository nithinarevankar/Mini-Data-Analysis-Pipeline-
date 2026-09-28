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

def date_check(data_engine):
    query ="SELECT load_date from bronze.metadata ORDER BY load_date DESC LIMIT 1"
    with data_engine.begin() as conn:
        result = conn.execute(text(query)).scalar()
    return result

def load_source_table(source_engine,data_engine,source_table: str,date_loaded):
    log.info(f"extracting {source_table}")
    df = pd.read_sql(f"SELECT * FROM {source_table} WHERE source_loaded_time > {date_loaded} ",source_engine)
    df["loaded_at"] = datetime.now(timezone.utc)
    df["source"]= source_table
    target_table = bronze_table_name(source_table)
    size = len(df)
    df.to_sql(
        target_table,
        data_engine,
        schema=BRONZE_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    log.info(f"loaded")
    return size

def meta_data(data_engine,source_table,size):
    with data_engine.begin() as conn:
        conn.execute(text(f"""CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.metadata(
                          id SERIAL,
                          source VARCHAR(50),
                          amt NUMERIC,
                          load_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP )"""))
        conn.execute(text(f"""INSERT INTO {BRONZE_SCHEMA}.metadata(
                                  source,amt
                                  ) VALUES('{source_table}',{size})"""))
 

def main():
    source_engine,data_engine = engine_creation()
    ensure_bronze_schema(data_engine)


    for source_table in SOURCE_TABLE:
        try:
            date_loaded = date_check(data_engine)
            size = load_source_table(source_engine,data_engine,source_table,date_loaded)
            meta_data(data_engine,source_table,size)
            
        except Exception as e:
            print("error")
            raise

if __name__ =="__main__":
    main()