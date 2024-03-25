import pandas as pd
import numpy as np
from psycopg2 import sql
import psycopg2
import os


def update_price():
    database_url = os.environ["DATABASE_URL"]
    connection = psycopg2.connect(database_url)
    cursor = connection.cursor()
    sales_data = pd.read_sql_query(
        """
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
    """,
        connection,
    )

    price_data = pd.read_sql_query(
        """
        SELECT * FROM prices
    """,
        connection,
    )

    df = (
        pd.merge_asof(
            sales_data.sort_values("timestamp"),
            price_data.sort_values("start_date"),
            left_on="timestamp",
            right_on="start_date",
            by="batch_name",
            direction="backward",
        )
        .sort_values(["batch_id", "timestamp"])
        .assign(stock_diff=lambda df: df.groupby("batch_id")["stock"].transform("diff"))
        .assign(
            sales=lambda df: np.where(
                df.timestamp < df.sell_by, df.price * df.stock_diff, 0
            )
        )
        .dropna(subset="sales")
        .sort_values(["product", "batch_id", "timestamp"])
    )

    price_ranges = {
        "rice": (1, 6),
        "wine": (4, 25),
        "apples-red": (1.2, 4),
        "apples-green": (1.2, 4),
        "bananas": (0.9, 3),
        "bananas-organic": (1.5, 3.5),
        "broccoli": (2, 6),
        "cheese": (7, 25),
        "beef": (10, 30),
        "avocado": (4, 12),
    }

    epsilon = 1

    print(
        f"epsilon set to {epsilon}, which means we explore {epsilon*100} % of the time"
    )

    unique_batch_names = df.sort_values("product")["batch_name"].unique()

    prices = []
    for batch_name in unique_batch_names:
        batch_df = df.loc[lambda df: df["batch_name"] == batch_name].reset_index(
            drop=True
        )
        product = batch_df.loc[0, "product"]
        if np.random.rand() > epsilon:
            # Exploit: select price with highest sales in history
            product_df = df.loc[lambda df: df["product"] == product]
            price = product_df["price"].agg("max")
            print(
                f"product {product}, batch {batch_name}: exploit. setting price to {price}"
            )
        else:
            # Explore: select random price
            price = np.random.uniform(
                low=price_ranges[product][0], high=price_ranges[product][1]
            )
            print(
                f"product {product}, batch {batch_name}: explore. sampling uniformly between "
                f"{price_ranges[product][0]} and {price_ranges[product][1]} eur. setting price to {price}"
            )
        prices.append((product, batch_name, price))

    prices_df = (
        pd.DataFrame(prices, columns=["product_name", "batch_name", "price"])
        .assign(start_date=pd.Timestamp.now())
        .sort_values(["product_name", "batch_name", "price"])
        .reset_index(drop=True)
    )

    insert_query = sql.SQL(
        "INSERT INTO prices (product_name, batch_name, price, start_date) VALUES (%s, %s, %s, %s)"
    )

    for row_index in range(len(prices_df)):
        cursor.execute(
            insert_query,
            (
                prices_df.product_name[row_index],
                prices_df.batch_name[row_index],
                prices_df.price[row_index],
                prices_df.start_date[row_index],
            ),
        )

    connection.commit()

    cursor.close()
    connection.close()
