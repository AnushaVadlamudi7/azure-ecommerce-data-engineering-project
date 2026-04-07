# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.ecommerceadls.dfs.core.windows.net",
  "acc.key"
)

# COMMAND ----------

sellers = spark.read.parquet(
    "abfss://silver@ecommerceadls.dfs.core.windows.net/ecommerce/sellers"
)

# COMMAND ----------

dim_sellers = sellers.select(
    "seller_id",
    "seller_city",
    "seller_state"
)

# COMMAND ----------

dim_sellers.write.mode("overwrite").parquet(
    "abfss://gold@ecommerceadls.dfs.core.windows.net/ecommerce/dim_sellers"
)
