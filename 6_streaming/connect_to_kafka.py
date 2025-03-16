#%%
import json

from kafka import KafkaProducer

#%%
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()


#%%
# read the data
import pandas as pd
data_df = pd.read_csv('data/green_tripdata_2019-10.csv')
data_df = data_df[['lpep_pickup_datetime',
'lpep_dropoff_datetime',
'PULocationID',
'DOLocationID',
'passenger_count',
'trip_distance',
'tip_amount']]

#%%
# data_df.dropna(inplace=True)
# data_df.lpep_pickup_datetime = pd.to_datetime(data_df.lpep_pickup_datetime)
# data_df.lpep_dropoff_datetime = pd.to_datetime(data_df.lpep_dropoff_datetime)
# data_df.passenger_count = data_df.passenger_count.astype(int)
#%%
import time
t0 = time.time()

topic_name = 'green-trips'

for i in range(data_df.shape[0]):
    message = data_df.iloc[i].to_dict()
    producer.send(topic_name, value=message)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
# %%
import csv
with open('data/green_tripdata_2019-10.csv', 'r', newline='', encoding='utf-8') as file:
    reader = csv.DictReader(file)

    for row in reader:
        # Each row will be a dictionary keyed by the CSV headers
        # Send data to Kafka topic "green-data"
        producer.send('green-data', value=row)

# Make sure any remaining messages are delivered
producer.flush()
producer.close()
# %%
