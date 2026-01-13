# Step 3: Incrementally ingest data with Auto Loader

from pyspark import pipelines as dp
from pyspark.sql.functions import *

path = "/Volumes/workspace/default/my_another_volume/raw_data/customers/"

# create the target bronze table
dp.create_streaming_table("customer_cdc_bronze", comment="New customer data incrementally ingested from cloud object storage landing zone")


@dp.append_flow(
    target = "customer_cdc_bronze"
    ,name = "customers_bronze_ingest_flow"
)

def customers_bronze_ingest_flow():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{path}")
    )