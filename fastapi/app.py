from asyncio import sleep
import datetime, os, google.auth.transport.requests, google.oauth2.id_token, requests, json, psycopg2
import time
import pandas as pd
import logging
import threading
from fastapi import FastAPI, HTTPException
from dataclasses import dataclass
from psycopg2 import sql
from model import update_price
import uvicorn


app = FastAPI()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    os.environ['GCP_SECRET']
)
audience = os.getenv("API_URL")
api_key = os.environ.get("API_KEY")
headers = {"X-API-KEY": api_key}

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class Batch:
    batch_name: str
    product_name: str
    batch_id: int
    price: int
    stock: int
    end_date: datetime


def __init__(self, Batches):
    self.Batches = Batches


def get_requests_headers(api_key):
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)

    return {
        "X-API-KEY": api_key,
        "Authorization": f"Bearer {id_token}",
    }


# All API calls to the database and exposed endpoint for the dynamic price simulator go here


@app.get("/")
def index():
    return "Hello to the PrAIce is Right Market"


@app.get("/prices", status_code=200)
def get_prices():
    connection = psycopg2.connect(os.environ["DATABASE_URL"])
    cursor = connection.cursor()

    select_query = (
        "SELECT * FROM prices WHERE start_date = (SELECT MAX(start_date) FROM prices)"
    )
    cursor.execute(select_query)

    rows = cursor.fetchall()
    data = {}

    for row in rows:
        product_name, batch_name, price, start_date, end_date = row

        if product_name not in data:
            data[product_name] = {}

        data[product_name][batch_name] = price

    json_data = json.dumps(data)
    cleaned_json_data = json.loads(json_data.replace("\\", ""))

    connection.commit()
    cursor.close()
    connection.close()
    return cleaned_json_data


@app.get("/prices/{batch_name}", status_code=200)
def get_product_price(batch_name: str):
    try:
        return {
            f"the price of {Batch[batch_name].product_name}: {Batch[batch_name].price}"
        }
    except KeyError:
        raise HTTPException(
            404, f"Product {Batch[batch_name].product_name} is not sold here, sorry"
        )


# All API calls to get data from the dynamic price simulator go here
@app.get("/audience_products")
def get_audience_products():
    headers = get_requests_headers(api_key)
    response = requests.get(f"{audience}/products", headers=headers).json()
    insert_batches(response)
    return response


@app.get("/audience_prices")
def get_audience_prices():
    headers = get_requests_headers(api_key)
    response = requests.get(f"{audience}/prices", headers=headers).json()
    insert_audience_prices(response)
    return response


@app.get("/audience_stocks")
def get_audience_stocks():
    headers = get_requests_headers(api_key)
    response = requests.get(f"{audience}/stocks", headers=headers).json()
    insert_stocks(response)
    return response


@app.get("/audience_leaderboards")
def get_audience_leaderboards():
    headers = get_requests_headers(api_key)
    response = requests.get(f"{audience}/leaderboards", headers=headers).json()
    insert_leaderboards(response)
    return response

def insert_leaderboards(leaderboards):
    connection = psycopg2.connect(os.environ["DATABASE_URL"])
    cursor = connection.cursor()

    execution_time = datetime.datetime.now()

    insert_query = sql.SQL(
        "INSERT INTO leaderboards (GenDP, DynamicDealmakers, random_competitor, RedAlert,ThePRIceIsRight, execution_time) VALUES (%s, %s, %s, %s,%s, %s)"
    )
    cursor.execute(insert_query, (leaderboards['GenDP'],leaderboards['DynamicDealmakers'],
                                  leaderboards['random_competitor'], leaderboards['RedAlert'],leaderboards['ThePRIceIsRight'],execution_time))


    # Commit the changes
    connection.commit()

    cursor.close()
    connection.close()



def insert_batches(batches):
    connection = psycopg2.connect(os.environ["DATABASE_URL"])
    cursor = connection.cursor()
    dict_key = "products"

    execution_time = datetime.datetime.now()

    for category, category_data in batches.items():
        for product, product_data in category_data[dict_key].items():
            batch_name = product
            batch_id = product_data["id"]
            sell_by = product_data["sell_by"]

            # Check if the product already exists in the database
            select_query = sql.SQL(
                "SELECT COUNT(*) FROM batchs WHERE batch_name = %s AND product = %s"
            )
            cursor.execute(select_query, (batch_name, category))
            count = cursor.fetchone()[0]

            # If the product doesn't exist, insert it into the database
            if count == 0:
                insert_query = sql.SQL(
                    "INSERT INTO batchs (batch_name, product, batch_id, sell_by, execution_time) VALUES (%s, %s, %s, %s, %s)"
                )
                cursor.execute(
                    insert_query,
                    (batch_name, category, batch_id, sell_by, execution_time),
                )

    # Commit the changes
    connection.commit()

    cursor.close()
    connection.close()


def insert_audience_prices(prices):
    connection = psycopg2.connect(os.environ["DATABASE_URL"])
    cursor = connection.cursor()
    table_name = "competitors"

    execution_time = datetime.datetime.now()

    for product, batches in prices.items():
        for batch, batch_data in prices[product].items():
            for competitor in batch_data:
                price = batch_data[competitor]
                insert_query = sql.SQL(
                    f"INSERT INTO {table_name} (batch_name, competitor, price, execution_time) VALUES (%s, %s, %s, %s)"
                )
                cursor.execute(
                    insert_query,
                    (batch, competitor, price, execution_time),
                )

    # Commit the changes
    connection.commit()

    cursor.close()
    connection.close()


def insert_stocks(stocks):
    connection = psycopg2.connect(os.environ["DATABASE_URL"])
    cursor = connection.cursor()

    execution_time = datetime.datetime.now()

    for category, category_data in stocks.items():
        for stock, stock_data in category_data.items():
            batch_id = stock
            amount = stock_data

            # Check if the product already exists in the database
            select_query = sql.SQL(
                "SELECT COUNT(*) FROM stock WHERE batch_id = %s AND execution_time = %s"
            )
            cursor.execute(select_query, (batch_id, execution_time))
            count = cursor.fetchone()[0]

            # If the product doesn't exist, insert it into the database
            if count == 0:
                insert_query = sql.SQL(
                    "INSERT INTO stock (batch_id, amount, execution_time) VALUES (%s, %s, %s)"
                )
                cursor.execute(
                    insert_query,
                    (batch_id, amount, execution_time),
                )

    # Commit the changes
    connection.commit()

    cursor.close()
    connection.close()


@app.get("/updates", status_code=200)
def updates():
    get_audience_products()
    get_audience_stocks()
    update_price()
    logger.info(f"Update has ran successfully.")
