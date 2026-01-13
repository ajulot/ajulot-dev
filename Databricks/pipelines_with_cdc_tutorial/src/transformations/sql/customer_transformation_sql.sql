create or refresh streaming table customers_cdc_bronze_sql
comment "New customer data incrementally ingested from cloud object storage landing zone";

create flow customers_bronze_ingest_flow_sql as 
insert into customers_cdc_bronze_sql by name
select * 
from stream read_files(
    "/Volumes/workspace/default/my_another_volume/raw_data/customers/",
    format => "json",
    inferColumnTypes => "true"
)

