# Step 4: Cleanup and expectations to track data quality

from pyspark import pipelines as dp
from pyspark.sql.functions import *


# create the target clean table
dp.create_streaming_table(name = "customers_cdc_clean"
                          , expect_all_or_drop = {"no_rescued_data": "_rescued_data IS NULL", "valid_id": "id IS NOT NULL", "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"}
)

@dp.append_flow(
    target = "customers_cdc_clean"
    ,name = "customers_cdc_clean_flow"
)
def customers_cdc_clean_flow():
    return (
        spark.readStream.table("customer_cdc_bronze")
            .select("address", "email", "id", "firstname", "lastname", "operation", "operation_date", "_rescued_data")
    )