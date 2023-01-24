FROM python:3

RUN pip install pandas psycopg2 sqlalchemy

WORKDIR /app

COPY pipelines.py pipelines.py
COPY ingest_data.py ingest_data.py
COPY yellow_taxi_data.csv yellow_taxi_data.csv

ENTRYPOINT ["python", "ingest_data.py"]



