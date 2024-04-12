# Databricks notebook source
# MAGIC %run /Workspace/Users/navallyemul@gmail.com/batch3/Day2/includes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step1

# COMMAND ----------

(spark.read.csv(f"{input_path}/super_store1.csv",header=True,inferSchema=True).write.option('delta.columnMapping.mode','name').mode("overwrite").saveAsTable("naval.superstore_bronze"))

# COMMAND ----------

df=spark.table("naval.superstore_bronze")

# COMMAND ----------

df1=df.dropDuplicates().dropna()

# COMMAND ----------

df1.write.option('delta.columnMapping.mode','name').mode("overwrite").saveAsTable("naval.superstore_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table naval.superstore_gold as 
# MAGIC select segment, sum(sales) as totalsales from naval.superstore_silver group by segment
