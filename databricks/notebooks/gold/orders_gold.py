# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.ecommerceadls.dfs.core.windows.net",
  "dK6Bq+R40gLWAtRzpG1SnztgK3TJHn9v9nopnUhdes3k5m+y9FJ2d1s2gBJQfaIL/FrwFtH/atih+AStRtLXWQ=="
)

# COMMAND ----------

orders = spark.read.parquet(
    "abfss://silver@ecommerceadls.dfs.core.windows.net/ecommerce/orders"
)

# COMMAND ----------

dim_orders = orders.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"

)

# COMMAND ----------

dim_orders.write.mode("overwrite").parquet(
    "abfss://gold@ecommerceadls.dfs.core.windows.net/ecommerce/dim_orders"
)