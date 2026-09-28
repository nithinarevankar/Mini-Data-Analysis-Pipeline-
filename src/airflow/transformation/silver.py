import os
import glob
from datetime import datetime, timezone, date
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv()

DATA_DB_CONN = os.getenv("DB_CONN_ENV")
SLIVER_SCHEME = "sliver"
BRONZE_SCHEMA = "bronze"

SOURCE_TABLE = [
    'sales'
]

def engine_creation():
    data_engine = create_engine(DATA_DB_CONN)
    return data_engine

def load(data_engine,source_table,last):
    print(last)
    data = pd.read_sql(f"SELECT * FROM {BRONZE_SCHEMA}.{source_table} WHERE loaded_at > {last} ",data_engine)
    return data

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

def load_silver(data_engine,data,source_table):
    data["loaded_at"] = datetime.now(timezone.utc)
    data["source"]= (f"{BRONZE_SCHEMA}.{source_table}")
    target_table = source_table
    size = len(data)
    data.to_sql(
            target_table,
            data_engine,
            schema=SLIVER_SCHEME,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
    return size

def meta_data(data_engine,source_table,size):
    with data_engine.begin() as conn:
        conn.execute(text(f"""CREATE TABLE IF NOT EXISTS {SLIVER_SCHEME}.metadata(
                          id SERIAL,
                          source VARCHAR(50),
                          amt NUMERIC,
                          load_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP )"""))
        conn.execute(text(f"""INSERT INTO {SLIVER_SCHEME}.metadata(
                                  source,amt
                                  ) VALUES('{BRONZE_SCHEMA}.{source_table}',{size})"""))

def date_check(data_engine):
    query ="SELECT load_date from sliver.metadata ORDER BY load_date DESC LIMIT 1"
    with data_engine.begin() as conn:
        result = conn.execute(text(query)).scalar()
    return result

def main():
    data_engine = engine_creation()
    for source_table in SOURCE_TABLE:
        try:
            last = date_check(data_engine)
            data = load(data_engine,source_table,last)
            if data.empty:
                print("no new data")
                size=0
            else:
              print("\n data before cleaning \n")
              print(eda_report(data))
              data = cleaning(data)
              print("\n data after cleaning \n")
              print(eda_report(data))
              size = load_silver(data_engine,data,source_table)
            meta_data(data_engine,source_table,size)

        except Exception as e:
            print("error")
            raise

if __name__ =="__main__":
    main()