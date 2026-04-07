# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.ecommerceadls.dfs.core.windows.net",
  "dK6Bq+R40gLWAtRzpG1SnztgK3TJHn9v9nopnUhdes3k5m+y9FJ2d1s2gBJQfaIL/FrwFtH/atih+AStRtLXWQ=="
)

# COMMAND ----------

customers = spark.read.parquet(
    "abfss://silver@ecommerceadls.dfs.core.windows.net/ecommerce/customers"
)

# COMMAND ----------

dim_customers = customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state"
)

# COMMAND ----------

dim_customers.write.mode("overwrite").parquet(
    "abfss://gold@ecommerceadls.dfs.core.windows.net/ecommerce/dim_customers"
)