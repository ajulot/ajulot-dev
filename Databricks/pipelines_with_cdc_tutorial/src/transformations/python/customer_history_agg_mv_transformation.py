# Step 7: Create a materialized view that tracks who has changed their information the most

from pyspark import pipelines as dp
from pyspark.sql.functions import *


# create the target mv
@dp.materialized_view(
    name = "customers_history_agg",
    comment = "Aggregated customer history"
)

def customers_history_agg():
    return (
        spark.read.table("customers_history")
            .groupBy("id")
            .agg(
                count("address").alias("address_count"),
                count("email").alias("email_count"),
                count("firstname").alias("firstname_count"),
                count("firstname").alias("lastname_count")
            )
    )