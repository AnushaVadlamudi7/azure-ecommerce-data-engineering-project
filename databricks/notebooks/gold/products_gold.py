# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.ecommerceadls.dfs.core.windows.net",
  "acc.key"

# COMMAND ----------

products = spark.read.parquet(
    "abfss://silver@ecommerceadls.dfs.core.windows.net/ecommerce/products"
)

# COMMAND ----------

dim_products = products.select(
    "product_id",
    "product_category_name",
    "product_name_length",
    "product_description_length",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
)

# COMMAND ----------

dim_products.write.mode("overwrite").parquet(
    "abfss://gold@ecommerceadls.dfs.core.windows.net/ecommerce/dim_products"
)
