FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_zone_data.py ingest_zone_data.py

ENTRYPOINT [ "python", "ingest_zone_data.py" ]