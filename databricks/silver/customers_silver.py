# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.ecommerceadls.dfs.core.windows.net",
  "dK6Bq+R40gLWAtRzpG1SnztgK3TJHn9v9nopnUhdes3k5m+y9FJ2d1s2gBJQfaIL/FrwFtH/atih+AStRtLXWQ=="
)

# COMMAND ----------

df=spark.read.format("parquet") \
    .load("abfss://raw@ecommerceadls.dfs.core.windows.net/ecommerce/customers.parquet")

# COMMAND ----------

display(df)
df.printSchema()

# COMMAND ----------

df = df.dropDuplicates(["customer_id"])

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col("customer_id").isNull()).count()

# COMMAND ----------

from pyspark.sql.functions import upper

df = df.withColumn("customer_city", upper(col("customer_city"))) \
       .withColumn("customer_state", upper(col("customer_state")))

# COMMAND ----------

df.write.mode("overwrite").parquet(
    "abfss://silver@ecommerceadls.dfs.core.windows.net/ecommerce/customers"
)