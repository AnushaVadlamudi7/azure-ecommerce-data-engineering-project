# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.ecommerceadls.dfs.core.windows.net",
  "account.key"
)

# COMMAND ----------

df=spark.read.format("parquet") \
    .load("abfss://raw@ecommerceadls.dfs.core.windows.net/ecommerce/sellers.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.dropDuplicates(["seller_id"])

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col("seller_id").isNull()).count()

# COMMAND ----------

from pyspark.sql.functions import upper

df = df.withColumn("seller_city", upper(col("seller_city"))) \
       .withColumn("seller_state", upper(col("seller_state")))

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").parquet(
    "abfss://silver@ecommerceadls.dfs.core.windows.net/ecommerce/sellers"
)
