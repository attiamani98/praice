import datetime, os, google, requests
from fastapi import FastAPI, HTTPException
from dataclasses import dataclass



app = FastAPI()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../pricing-prd-11719402-69eaf79e6222.json'
audience = os.getenv("API_URL")
api_key = os.environ.get("API_KEY")

headers = {"X-API-KEY": api_key}

@dataclass
class Batch():
    batch_name: str
    product_name: str
    batch_id: int
    price: int
    stock: int
    end_date: datetime


def __init__(self, Batches):
        self.Batches = Batches

@app.get("/")
def index():
  return "Hello to the PrAIce is Right Market"

@app.get("/prices", status_code=200)
def get_prices(self):
    for batch in self.Batches :
     return batch.price


@app.get("/prices/{batch_name}", status_code=200)
def get_product_price(batch_name: str):
    try:
        return {f'the price of {Batch[batch_name].product_name}: {Batch[batch_name].price}'}
    except KeyError:
        raise HTTPException(404, f"Product {Batch[batch_name].product_name} is not sold here, sorry")


def get_requests_headers(api_key):
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)

    return {
        "X-API-KEY": api_key,
        "Authorization": f"Bearer {id_token}",
    }


def get_audience_products():
    headers = get_requests_headers(api_key)
    requests.get(f"{audience}/products", headers=headers).json()