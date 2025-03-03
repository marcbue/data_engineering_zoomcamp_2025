#%%
import os
from pathlib import Path
from dotenv import load_dotenv
import findspark
env_path = Path(os.getenv("HOME")) / "data_engineering_zoomcamp_2025" / "5_batch" / ".env"
load_dotenv(dotenv_path=env_path)
findspark.init()

#%%
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')

df.show()

# %%
df.write.parquet('zones')
