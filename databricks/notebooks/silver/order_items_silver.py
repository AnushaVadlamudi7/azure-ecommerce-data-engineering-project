# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.ecommerceadls.dfs.core.windows.net",
  "account.key"
)

# COMMAND ----------

df=spark.read.format("parquet") \
    .load("abfss://raw@ecommerceadls.dfs.core.windows.net/ecommerce/order_items.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df = df.withColumn("price", col("price").cast("double")) \
       .withColumn("freight_value", col("freight_value").cast("double")) \
       .withColumn("shipping_limit_date",col("shipping_limit_date").cast("timestamp"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------


df = df.dropDuplicates(["order_id", "order_item_id"])

# COMMAND ----------

df.filter(col("order_id").isNull()).count()

# COMMAND ----------

df.write.mode("overwrite").parquet(
    "abfss://silver@ecommerceadls.dfs.core.windows.net/ecommerce/order_items"
)
