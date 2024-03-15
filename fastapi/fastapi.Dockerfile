
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app
RUN pip install -r requirements.txt
COPY . /app
RUN pip install -r requirements.txt
EXPOSE 8000

ENTRYPOINT ["uvicorn", "app:app"]
