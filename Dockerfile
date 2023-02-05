FROM python:3

RUN pip install pandas psycopg2 sqlalchemy

WORKDIR /app

COPY pipelines.py pipelines.py
COPY ingest_data.py ingest_data.py
COPY files/yellow_tripdata_2021-07.csv yellow_tripdata_2021-07.csv

ENTRYPOINT ["python", "ingest_data.py"]