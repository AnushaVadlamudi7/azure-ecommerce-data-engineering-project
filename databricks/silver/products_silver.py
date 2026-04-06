# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.ecommerceadls.dfs.core.windows.net",
  "dK6Bq+R40gLWAtRzpG1SnztgK3TJHn9v9nopnUhdes3k5m+y9FJ2d1s2gBJQfaIL/FrwFtH/atih+AStRtLXWQ=="
)

# COMMAND ----------

df=spark.read.format("parquet") \
    .load("abfss://raw@ecommerceadls.dfs.core.windows.net/ecommerce/products.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumnRenamed("product_name_lenght", "product_name_length") \
       .withColumnRenamed("product_description_lenght", "product_description_length")

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col("product_category_name").isNull()).count()

# COMMAND ----------

from pyspark.sql.functions import lit

df = df.fillna({"product_category_name": "UNKNOWN"})

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col("product_category_name").isNull()).count()

# COMMAND ----------

df = df.dropDuplicates(["product_id"])

# COMMAND ----------

df = df.withColumn("product_weight_g", col("product_weight_g").cast("int")) \
       .withColumn("product_length_cm", col("product_length_cm").cast("int")) \
       .withColumn("product_height_cm", col("product_height_cm").cast("int")) \
       .withColumn("product_width_cm", col("product_width_cm").cast("int"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.mode("overwrite").parquet(
    "abfss://silver@ecommerceadls.dfs.core.windows.net/ecommerce/products"
)