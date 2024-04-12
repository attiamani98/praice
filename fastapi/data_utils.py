import os

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


def load_data(from_date):
    database_url = os.environ["DATABASE_URL"]
    engine = create_engine(database_url)

    sales_data_query = f"""
        SELECT
            st.batch_id,
            b.batch_name,
            st.execution_time AS timestamp,
            b.product AS product,
            b.sell_by AS sell_by,
            st.amount AS stock
        FROM
            stock st
        JOIN
            batchs b ON CAST(b.batch_id AS TEXT) = CAST(st.batch_id AS TEXT)
        WHERE st.execution_time > '{from_date}'
    """
    sales_data = pd.read_sql_query(sales_data_query, engine)

    price_data_query = """
        SELECT product_name, batch_name, price, start_date FROM prices
    """
    price_data = pd.read_sql_query(price_data_query, engine)

    competitor_data_query = """
        SELECT * FROM competitors
    """
    competitor_data = pd.read_sql_query(competitor_data_query, engine)

    return sales_data, price_data, competitor_data

def load_data_current_batches():
    database_url = os.environ["DATABASE_URL"]
    engine = create_engine(database_url)

    sales_data_query = """
        SELECT
            st.batch_id,
            b.batch_name,
            st.execution_time AS timestamp,
            b.product AS product,
            b.sell_by AS sell_by,
            st.amount AS stock
        FROM
            stock st
        JOIN
            batchs b ON CAST(b.batch_id AS TEXT) = CAST(st.batch_id AS TEXT)
        WHERE b.batch_name IN (SELECT DISTINCT batch_name FROM stock WHERE sell_by > NOW() AND execution_time = (SELECT MAX(execution_time) FROM stock))
    """
    sales_data = pd.read_sql_query(sales_data_query, engine)

    price_data_query = """
        SELECT product_name, batch_name, price, start_date FROM prices
    """
    price_data = pd.read_sql_query(price_data_query, engine)

    competitor_data_query = """
        SELECT * FROM competitors
    """
    competitor_data = pd.read_sql_query(competitor_data_query, engine)

    return sales_data, price_data, competitor_data

def preprocess_data(sales_data, price_data, competitor_data):

    sales_and_price_merged = pd.merge_asof(
        sales_data.sort_values("timestamp"),
        price_data.sort_values("start_date"),
        left_on="timestamp",
        right_on="start_date",
        by="batch_name",
        direction="backward"
    )

    competitor_data_preprocessed = (
        competitor_data
        .pivot(
            index=['batch_name', 'execution_time'], 
            columns='competitor', 
            values='price'
        )
        .reset_index()
        .rename(columns=lambda columns: columns.lower())
        
    )

    return (
        pd.merge_asof(
            sales_and_price_merged.sort_values("timestamp"),
            competitor_data_preprocessed.sort_values("execution_time"),
            left_on="timestamp",
            right_on="execution_time",
            by="batch_name",
            direction="backward"
        )
        .assign(stock_diff = lambda df: -df.groupby('batch_id')['stock'].transform('diff'))
        .assign(sales = lambda df: np.where(df.timestamp < df.sell_by, df.price * df.stock_diff, 0))
        .assign(hours_until_perished = lambda df: (df.timestamp - df.sell_by).dt.seconds / 60 / 60)
    )

def update_prices_db(prices_df):
    database_url = os.environ["DATABASE_URL"]
    engine = create_engine(database_url)

    insert_query = """
        INSERT INTO prices (product_name, batch_name, price, start_date) VALUES (:product_name, :batch_name, :price, :start_date)
    """

    # Using a connection from the engine to execute each insert operation in a loop
    with engine.connect() as connection:
        for row_index, row in prices_df.iterrows():
            connection.execute(
                text(insert_query),  # Convert string SQL query to a SQL Alchemy TextClause
                {
                    "product_name": row['product_name'],
                    "batch_name": row['batch_name'],
                    "price": row['price'],
                    "start_date": row['start_date'],
                }
            )
        connection.commit()

    print("prices updated succesfully")