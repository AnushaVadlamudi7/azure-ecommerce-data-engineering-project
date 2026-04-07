# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.ecommerceadls.dfs.core.windows.net",
  "dK6Bq+R40gLWAtRzpG1SnztgK3TJHn9v9nopnUhdes3k5m+y9FJ2d1s2gBJQfaIL/FrwFtH/atih+AStRtLXWQ=="
)

# COMMAND ----------

display(
  dbutils.fs.ls("abfss://raw@ecommerceadls.dfs.core.windows.net/ecommerce/")
)

# COMMAND ----------

df = spark.read.format("parquet") \
    .load("abfss://raw@ecommerceadls.dfs.core.windows.net/ecommerce/orders.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df = df.withColumn(
    "order_purchase_timestamp",
    col("order_purchase_timestamp").cast("timestamp")
).withColumn(
    "order_delivered_customer_date",
    col("order_delivered_customer_date").cast("timestamp")
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.dropDuplicates(["order_id"])

# COMMAND ----------

df.filter(col("order_id").isNull()).count()

# COMMAND ----------

df.write.mode("overwrite").parquet(
    "abfss://silver@ecommerceadls.dfs.core.windows.net/ecommerce/orders"
)