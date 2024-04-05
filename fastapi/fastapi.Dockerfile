FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY pricing-prd-11719402-69eaf79e6222.json /app
COPY . /app
RUN pip install -r requirements.txt
EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--reload"]
