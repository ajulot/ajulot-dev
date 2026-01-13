# Step 5: Materialize the customers table with an AUTO CDC flow

from pyspark import pipelines as dp
from pyspark.sql.functions import *


# create the target clean table
dp.create_streaming_table(name = "customers"
                          , comment = "Clean, materialized customers"
)

dp.create_auto_cdc_flow(
    target="customers",
    source="customers_cdc_clean",
    keys=["id"],
    sequence_by=col("operation_date"),
    ignore_null_updates=False,
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "operation_date", "_rescued_data"],
)
