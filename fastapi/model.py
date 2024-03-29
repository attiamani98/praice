import numpy as np
from sqlalchemy import create_engine, text
import pandas as pd
import os

def load_data():
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
    """
    sales_data = pd.read_sql_query(sales_data_query, engine)

    price_data_query = """
        SELECT product_name, batch_name, price, start_date FROM prices
    """
    price_data = pd.read_sql_query(price_data_query, engine)

    return sales_data, price_data

def preprocess_data(sales_data, price_data):
    return (
        pd.merge_asof(
            sales_data.sort_values("timestamp"),
            price_data.sort_values("start_date"),
            left_on="timestamp",
            right_on="start_date",
            by="batch_name",
            direction="backward"
        )
        .assign(stock_diff = lambda df: -df.groupby('batch_id')['stock'].transform('diff'))
        .assign(sales = lambda df: np.where(df.timestamp < df.sell_by, df.price * df.stock_diff, 0))
        .dropna(subset='sales')
        .sort_values(['product', 'batch_id', 'timestamp'])
    )

def greedy_epsilon(sales_data, preprocessed_df, epsilon=0.35):

    price_ranges = {
        'rice': (1, 6),
        'wine': (4, 25),
        'apples-red': (1.2, 4),
        'apples-green': (1.2, 4),
        'bananas': (0.9, 3),
        'bananas-organic': (1.5, 3.5),
        'broccoli': (2, 6),
        'cheese': (7, 25),
        'beef': (10, 30),
        'avocado': (4, 12)
    }

    print(f'epsilon set to {epsilon}, which means we explore {epsilon*100} % of the time')

    unique_batch_names = sales_data.loc[lambda df: df.sell_by > pd.Timestamp.now()]['batch_name'].unique()

    prices = []
    for batch_name in unique_batch_names:
        product = sales_data.loc[lambda df: df['batch_name'] == batch_name]['product'].values[0]
        if np.random.rand() > epsilon:
            product_df = preprocessed_df.loc[lambda df: df['product'] == product]
            price = product_df.iloc[product_df['sales'].argmax()]['price']
            print(f'product {product}, batch {batch_name}: exploit. setting price to {price} '
                f'which generated the highest sales of {product_df['sales'].max()}')
        else:
            price = np.random.uniform(low=price_ranges[product][0], high=price_ranges[product][1])
            print(f'product {product}, batch {batch_name}: explore. sampling uniformly between '
                f'{price_ranges[product][0]} and {price_ranges[product][1]} eur. setting price to {price}')
        prices.append((product, batch_name, price))

    prices_df = (
        pd.DataFrame(prices, columns=['product_name', 'batch_name', 'price'])
        .assign(start_date = pd.Timestamp.now())
        .sort_values(['product_name', 'batch_name', 'price'])
        .reset_index(drop=True)
    )

    return prices_df

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

def update_price(update_db=True):
    sales_data, price_data = load_data()
    preprocessed_df = preprocess_data(sales_data, price_data)
    prices_df = greedy_epsilon(sales_data, preprocessed_df)
    if update_db:
        update_prices_db(prices_df)