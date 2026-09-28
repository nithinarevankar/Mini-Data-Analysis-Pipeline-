import os
import pandas as pd

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATA_DB_CONN = os.getenv("DB_CONN_ENV")

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SOURCE_TABLE = [
    "sales"
]


def engine_creation():
    return create_engine(DATA_DB_CONN)


def table_creation(data_engine):

    with data_engine.begin() as conn:

        # Make sure Gold schema exists
        conn.execute(text(f"""
            CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.gold_payment_table (
                payment_method VARCHAR(50),
                count_method BIGINT,
                total_sales NUMERIC,
                total_quantity BIGINT,
                avg_order_value NUMERIC
            );
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.gold_category_table (
                category TEXT,
                count_method BIGINT,
                total_sales NUMERIC,
                total_quantity BIGINT,
                avg_order_value NUMERIC
            );
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.gold_item_table (
                item VARCHAR(50),
                count_method BIGINT,
                total_sales NUMERIC,
                total_quantity BIGINT,
                avg_order_value NUMERIC
            );
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.gold_customer_table (
                customer_id VARCHAR(50) NOT NULL,
                count_method BIGINT,
                total_sales NUMERIC,
                total_quantity BIGINT,
                avg_order_value NUMERIC
            );
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.gold_summary (
                stats TEXT,
                values TEXT
            );
        """))


def gold_transformation(data_engine, source_table):

    with data_engine.begin() as conn:

        conn.execute(text(f"""
            TRUNCATE TABLE
                {GOLD_SCHEMA}.gold_category_table,
                {GOLD_SCHEMA}.gold_payment_table,
                {GOLD_SCHEMA}.gold_item_table,
                {GOLD_SCHEMA}.gold_customer_table;
        """))



        conn.execute(text(f"""
            INSERT INTO {GOLD_SCHEMA}.gold_category_table
            (
                category,
                count_method,
                total_sales,
                total_quantity,
                avg_order_value
            )
            SELECT
                "Category",
                COUNT(*) AS count_method,
                SUM("Total Spent"::NUMERIC) AS total_sales,
                SUM("Quantity"::NUMERIC) AS total_quantity,
                AVG("Total Spent"::NUMERIC) AS avg_order_value
            FROM {SILVER_SCHEMA}.{source_table}
            GROUP BY "Category";
        """))


        conn.execute(text(f"""
            INSERT INTO {GOLD_SCHEMA}.gold_payment_table
            (
                payment_method,
                count_method,
                total_sales,
                total_quantity,
                avg_order_value
            )
            SELECT
                "Payment Method",
                COUNT(*) AS count_method,
                SUM("Total Spent"::NUMERIC) AS total_sales,
                SUM("Quantity"::NUMERIC) AS total_quantity,
                AVG("Total Spent"::NUMERIC) AS avg_order_value
            FROM {SILVER_SCHEMA}.{source_table}
            GROUP BY "Payment Method";
        """))


        conn.execute(text(f"""
            INSERT INTO {GOLD_SCHEMA}.gold_item_table
            (
                item,
                count_method,
                total_sales,
                total_quantity,
                avg_order_value
            )
            SELECT
                "Item",
                COUNT(*) AS count_method,
                SUM("Total Spent"::NUMERIC) AS total_sales,
                SUM("Quantity"::NUMERIC) AS total_quantity,
                AVG("Total Spent"::NUMERIC) AS avg_order_value
            FROM {SILVER_SCHEMA}.{source_table}
            GROUP BY "Item";
        """))

        conn.execute(text(f"""
            INSERT INTO {GOLD_SCHEMA}.gold_customer_table
            (
                customer_id,
                count_method,
                total_sales,
                total_quantity,
                avg_order_value
            )
            SELECT
                "Customer ID",
                COUNT(*) AS count_method,
                SUM("Total Spent"::NUMERIC) AS total_sales,
                SUM("Quantity"::NUMERIC) AS total_quantity,
                AVG("Total Spent"::NUMERIC) AS avg_order_value
            FROM {SILVER_SCHEMA}.{source_table}
            GROUP BY "Customer ID";
        """))




def meta_data(data_engine,source_table):
    with data_engine.begin() as conn:
        conn.execute(text(f"""CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.metadata(
                          id SERIAL,
                          source VARCHAR(50),
                          load_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP )"""))
        conn.execute(text(f"""INSERT INTO {GOLD_SCHEMA}.metadata(
                                  source
                                  ) VALUES('{SILVER_SCHEMA}.{source_table}')"""))

def main():

    data_engine = engine_creation()

    for source_table in SOURCE_TABLE:

        try:

            table_creation(data_engine)

          

            gold_transformation(
                data_engine,
                source_table
            )
            meta_data(data_engine,source_table)
            print(
                f"Gold transformation completed for {source_table}"
            )

        except Exception as e:

            print(f"Error processing {source_table}: {e}")

            raise


if __name__ == "__main__":
    main()