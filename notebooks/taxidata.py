# Databricks notebook source
#filePath = "/mnt/rawtaxidata1"
#taxidataRaw = spark.readStream.format("delta").option("header","true").option("inferSchema", "true").load(filePath)

taxidataRaw = spark.readStream.format("delta").table("taxidataRaw")

# COMMAND ----------

taxidataRaw.printSchema

# COMMAND ----------

display(taxidataRaw)

# COMMAND ----------

from pyspark.sql.functions import hour, mean

display(taxidataRaw.groupBy(hour("lpep_pickup_datetime").alias("hour")).count())

# COMMAND ----------

from pyspark.sql.functions import hour, mean

display(taxidata.groupBy("VendorID",hour("lpep_pickup_datetime").alias("hour")).count())

# COMMAND ----------

# MAGIC %fs ls "/mnt/rawtaxidata1"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxidataRaw;
# MAGIC CREATE TABLE taxidataRaw
# MAGIC USING delta
# MAGIC AS SELECT *
# MAGIC FROM delta.`/mnt/rawtaxidata1`

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from taxidataRaw

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxidataRaw 

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE taxidataRaw SET VendorID = 'YellowCab' WHERE VendorID = '2'

# COMMAND ----------

