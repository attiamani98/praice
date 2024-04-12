import numpy as np
import pandas as pd
from data_utils import load_data, load_data_current_batches, preprocess_data, update_prices_db

def greedy_epsilon(df, epsilon=0.35):

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

    unique_batch_names = df.loc[lambda df: df.sell_by > pd.Timestamp.now()]['batch_name'].unique()

    prices = []
    for batch_name in unique_batch_names:
        product = df.loc[lambda df: df['batch_name'] == batch_name]['product'].values[0]
        if np.random.rand() > epsilon:
            product_df = df.loc[lambda df: df['product'] == product]
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

def undercut_min_price(df):

    return (
        df
        .assign(start_stock = df.groupby('batch_name')['stock'].transform("max"))
        .assign(stock_per = lambda df: df.stock / df.start_stock)
        .loc[lambda df: df.timestamp == df.timestamp.max()]
        .loc[lambda df: df.sell_by > pd.Timestamp.now()]
        .set_index(['product', 'batch_name', 'stock_per'])
        [['dynamicdealmakers', 'gendp', 'redalert', 'random_competitor']]
        .min(axis=1)
        .rename('price')
        .reset_index()
        .assign(price = lambda df: np.where(df.stock_per < 0.15, df.price * 20, df.price - 0.03))
        .rename(columns={'product': 'product_name'})
        .sort_values(['product_name', 'batch_name', 'price'])
        .assign(start_date = pd.Timestamp.now())
        .reset_index(drop=True)
        .fillna(5)
    )


def update_price(update_db=True):
    sales_data, price_data, competitor_data = load_data_current_batches()
    df = preprocess_data(sales_data, price_data, competitor_data)
    prices_df = undercut_min_price(df)
    if update_db:
        update_prices_db(prices_df)