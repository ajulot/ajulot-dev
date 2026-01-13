# Step 6: Track update history with slowly changing dimension type 2 (SCD2)

from pyspark import pipelines as dp
from pyspark.sql.functions import *


# create the target clean table
dp.create_streaming_table(name = "customers_history"
                          , comment = "SCD2 for customers"
)

dp.create_auto_cdc_flow(
    target="customers_history",
    source="customers_cdc_clean",
    keys=["id"],
    sequence_by=col("operation_date"),
    ignore_null_updates=False,
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "operation_date", "_rescued_data"],
    stored_as_scd_type="2",
)
