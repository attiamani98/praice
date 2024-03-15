from fastapi import FastAPI, HTTPException
from dataclasses import dataclass



app = FastAPI()

@dataclass
class Product():
    name: str
    price: int

def __init__(self, Products):
        self.Products = Products

@app.get("/")
def index():
  return "Hello to the PrAIce is Right Market"

@app.get("/prices", status_code=200)
def get_prices(self):
    for product in self.Products :
     return product.price


app.get("/prices/{product_name}", status_code=200)
def get_product_price(product_name: str):
    try:
        return {'the price of ${product_name}:', Product[product_name].price}
    except KeyError:
        raise HTTPException(404, f"Product {product_name} is not sold here, sorry")