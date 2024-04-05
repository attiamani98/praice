FROM python:3.12-slim

WORKDIR /app

ARG GCP_SECRET
ENV GCP_SECRET=$GCP_SECRET

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY . /app
RUN pip install -r requirements.txt
EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--reload"]
