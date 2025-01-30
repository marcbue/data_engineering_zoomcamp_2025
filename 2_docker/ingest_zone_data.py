import pandas as pd
import argparse
import os
from sqlalchemy import create_engine
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    print(url)
    
    csv_name = url.split("/")[-1]
    
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    
    t_start = time()
    df = pd.read_csv(csv_name)
    df.to_sql(name=table_name, con=engine, if_exists='append')
    t_end = time()
    
    print(f'inserted data took {(t_end-t_start):.3f}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')
    
    args = parser.parse_args()
    
    main(args)






