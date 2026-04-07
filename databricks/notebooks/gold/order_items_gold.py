# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.ecommerceadls.dfs.core.windows.net",
  "acc.key"
)

# COMMAND ----------

order_items = spark.read.parquet(
    "abfss://silver@ecommerceadls.dfs.core.windows.net/ecommerce/order_items"
)

orders = spark.read.parquet(
    "abfss://silver@ecommerceadls.dfs.core.windows.net/ecommerce/orders"
)

# COMMAND ----------

fact_df = order_items.join(
    orders,
    on="order_id",
    how="inner"
)

# COMMAND ----------

fact_order_items = fact_df.select(
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "customer_id",
    "order_purchase_timestamp",
    "price",S
    "freight_value"
)

# COMMAND ----------

fact_order_items.write.mode("overwrite").parquet(
    "abfss://gold@ecommerceadls.dfs.core.windows.net/ecommerce/fact_order_items"
)
