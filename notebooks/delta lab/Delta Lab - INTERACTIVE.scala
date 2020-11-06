// Databricks notebook source
// MAGIC %md
// MAGIC # Welcome Ignite 2019
// MAGIC ## WRK2013 - Hands-On With Azure Databricks Delta Lake

// COMMAND ----------

// MAGIC %md
// MAGIC <img src="https://github.com/kywe665/HelpImageHost/blob/master/LabContents.png?raw=true" alt="drawing" width="960"/>

// COMMAND ----------

// MAGIC %md
// MAGIC <img src="https://github.com/kywe665/HelpImageHost/blob/master/Arch.png?raw=true" alt="drawing" width="1000"/>

// COMMAND ----------

// MAGIC %md
// MAGIC <img src="https://github.com/kywe665/HelpImageHost/blob/master/DeltaLake.png?raw=true" alt="drawing" width="960"/>

// COMMAND ----------

// MAGIC %md
// MAGIC #### We are going to use a dataset of IoT sensor readings
// MAGIC <img src="https://github.com/kywe665/HelpImageHost/blob/master/photo-power-plant.jpg?raw=true" alt="drawing" width="600"/>

// COMMAND ----------

// MAGIC %md
// MAGIC #### First lets take a peek at this data:

// COMMAND ----------

dbutils.fs.head("dbfs:/databricks-datasets/iot/iot_devices.json")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Now we can read it in as a dataframe:

// COMMAND ----------

// DBTITLE 0,Load Sample Data
val rawPath = "dbfs:/databricks-datasets/iot/iot_devices.json"

val deviceRaw = spark.read.json(rawPath)
display(deviceRaw)

// COMMAND ----------

// MAGIC %md ### Create Table ###
// MAGIC 
// MAGIC Let us create a Delta Table using the records we have read in. You can use existing Spark SQL code and change the format from parquet, csv, json, and so on, to delta.

// COMMAND ----------

//write data to Delta Lake file
deviceRaw.write.format("delta").mode("overwrite").save("/delta/rawDevices")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Let's explore exactly what files were written in that transaction:

// COMMAND ----------

// MAGIC %fs ls "/delta/rawDevices/"

// COMMAND ----------

// MAGIC %md
// MAGIC <img src="https://github.com/kywe665/HelpImageHost/blob/master/DeltaLake-TransactionLog1.png?raw=true" alt="drawing" width="600"/>
// MAGIC <img src="https://github.com/kywe665/HelpImageHost/blob/master/DeltaLake-TransactionLog2.png?raw=true" alt="drawing" width="600"/>

// COMMAND ----------

// MAGIC %md
// MAGIC #### Create a Databricks Delta table directly from the files

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS deviceRaw;
// MAGIC CREATE TABLE deviceRaw
// MAGIC USING delta
// MAGIC AS SELECT *
// MAGIC FROM delta.`/delta/rawDevices`

// COMMAND ----------

// MAGIC %md
// MAGIC ####Read the table
// MAGIC Explore the details and extra features that a Delta Table provides

// COMMAND ----------

// USING TABLE NAME
val raw_Delta_Table = spark.table("deviceRaw")

display(raw_Delta_Table)

// COMMAND ----------

// MAGIC %md 
// MAGIC Once our dataframe from Delta source is generated, we can click on the dataframe name in the notebook state panel to view the metadata related to our delta table. The metadata includes:
// MAGIC 
// MAGIC 
// MAGIC 1. Schema
// MAGIC 
// MAGIC ![schema](https://i.imgur.com/3pTqz6P.png)
// MAGIC 
// MAGIC 2. Details
// MAGIC 
// MAGIC ![details](https://i.imgur.com/9rbBfNr.png)
// MAGIC 
// MAGIC 3. History
// MAGIC 
// MAGIC ![history](https://i.imgur.com/us1a1FU.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Append data to a table ###
// MAGIC 
// MAGIC As new events arrive, you can atomically append them to the table. Let us create a new record that we will append to our table.

// COMMAND ----------

import org.apache.spark.sql.types._

val newRecord = Seq((8,
                     867L,
                     "US",
                     "USA",
                     "United States",
                     9787236987276352L,
                     "meter-gauge-1xbYRYcjdummy",
                     51L,
                     "68.161.225.1",
                     38.000000,
                     "green",
                     -97.000000,
                     "Celsius",
                     34L,
                     1458444054093L
                    ))
.toDF("battery_level", "c02_level", "cca2", "cca3", "cn", "device_id", "device_name", "humidity", "ip", "latitude", "lcd", "longitude", "scale", "temp", "timestamp")

display(newRecord)

// COMMAND ----------

// MAGIC %md
// MAGIC Append this record directly into our delta table

// COMMAND ----------

newRecord
  .write
  .format("delta")
  .mode("append")
  .saveAsTable("deviceRaw")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Failed Schema Merge? THANK YOU DELTA for enforcing the schema!!
// MAGIC Repeat below after correcting the schema. 
// MAGIC 
// MAGIC You can also tell Delta to merge schemas, or overwrite schemas, but it prevents unintentional schema changes or type mismatches

// COMMAND ----------

import org.apache.spark.sql.types._

val newRecord = Seq((8L,
                     867L,
                     "US",
                     "USA",
                     "United States",
                     9787236987276352L,
                     "meter-gauge-1xbYRYcjdummy",
                     51L,
                     "68.161.225.1",
                     38.000000,
                     "green",
                     -97.000000,
                     "Celsius",
                     34L,
                     1458444054093L
                    ))
.toDF("battery_level", "c02_level", "cca2", "cca3", "cn", "device_id", "device_name", "humidity", "ip", "latitude", "lcd", "longitude", "scale", "temp", "timestamp")

display(newRecord)

// COMMAND ----------

newRecord
  .write
  .format("delta")
  .mode("append")
  .saveAsTable("deviceRaw")

// COMMAND ----------

// MAGIC %md We can do the same thing using SQL

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO deviceRaw VALUES (8L, 867L, "US", "USA", "United States", 1787236987276352L, "meter-gauge-1xbYRYcjdummy", 51L, "68.161.225.1", 38.000000, "green", -97.000000, "Celsius", 34L, 1458444054093L)

// COMMAND ----------

// MAGIC %md
// MAGIC Let us perform a count operation on the delta table to see the updated count after append operation.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM deviceRaw

// COMMAND ----------

// MAGIC %md 
// MAGIC ##DATABRICKS DELTA WITH STRUCTURED STREAMING##
// MAGIC ### Concurrency
// MAGIC 
// MAGIC Let's put a stream reader on the deviceRaw table. This will continously monitor the files for new data, then stream and append this data to our new "streamingDevices" path.

// COMMAND ----------

dbutils.fs.rm("/delta/_checkpoint",true)
dbutils.fs.rm("/delta/streamingDevices",true)

val newData = spark.readStream.table("deviceRaw")

newData.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/delta/_checkpoint")
  .option("ignoreDeletes", true)
  .option("ignoreChanges", true)
  .start("/delta/streamingDevices/")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Observe the stream processing

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

display(
  spark.readStream.format("delta")
  .load("/delta/streamingDevices/")
  .groupBy($"cn")
  .agg(sum($"battery_level").as("battery_level"))
  .orderBy($"battery_level".desc)
 )

// COMMAND ----------

// MAGIC %md
// MAGIC #### Add rows and see them get processed right away

// COMMAND ----------

val newRecord = Seq((999999L,
                     86L,
                     "NA",
                     "NAN",
                     "MY FAKE COUNTRY",
                     9787236987276352L,
                     "HELLO WORLD STREAM",
                     5L,
                     "68.161.225.1",
                     0.000000,
                     "test",
                     0.000000,
                     "Kelvin",
                     34L,
                     1458444054093L
                    ))
.toDF("battery_level", "c02_level", "cca2", "cca3", "cn", "device_id", "device_name", "humidity", "ip", "latitude", "lcd", "longitude", "scale", "temp", "timestamp")

newRecord
  .write
  .format("delta")
  .mode("append")
  .saveAsTable("deviceRaw")

// COMMAND ----------

// MAGIC %md
// MAGIC ##UPSERTS AND  UPDATES##
// MAGIC Unlike the file APIs in Apache Spark, Databricks Delta remembers and enforces the schema of a table. This means that by default overwrites do not replace the schema of an existing table.

// COMMAND ----------

// MAGIC %md
// MAGIC #### You are analyzing data and you notice that the US c02_levels are abnormally high above the rest

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT c02_level, deviceRaw.cn, timestamp
// MAGIC FROM deviceRaw 
// MAGIC WHERE cn IN ("United States", "United Kingdom", "China", "Japan", "Brazil")

// COMMAND ----------

// MAGIC %md
// MAGIC #### After discussing in depth with engineers responsible for the IoT sensors themselves you discover that the devices in US were configured to report c02_levels as milligram per cubic meter, instead of reporting as parts per million. 
// MAGIC Because of Avogadros number and PV=nRT, you can adjust these levels back to normal by dividing by 1.8. (*Not Acurate Science, just roleplay*) https://www.co2meter.com/blogs/news/15164297-co2-gas-concentration-defined

// COMMAND ----------

// MAGIC %sql
// MAGIC UPDATE deviceRaw SET c02_level = c02_level/1.8 WHERE cn = 'United States'

// COMMAND ----------

// MAGIC %md
// MAGIC #### Looks like everything is corrected back to normal now:

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT c02_level, deviceRaw.cn, timestamp
// MAGIC FROM deviceRaw 
// MAGIC WHERE cn IN ("United States", "United Kingdom", "China", "Japan", "Brazil")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Let's say you work at a large corporation and you own a centralized data platform that 100's of employees around the globe are using. 
// MAGIC Let's say that you didn't communicate this change very well to all those who were consuming this data. Thank you Delta for audit history:

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY deviceRaw

// COMMAND ----------

// MAGIC %md
// MAGIC #### Now your data science team tracks you down from the Delta audit history and is mad. 
// MAGIC They have an ML Model deployed that predicts c02 levels and decides when to shut down certain plant operations based on those predictions. They were tracking their model accuracy via Azure Machine Learning and around the time you made changes to the data, their models started performing poorly. They have now learned this was because of your changes and are mad because now they have to build new models. They have to start training from scratch and they don't have any reproducible baselines to compare with because you already changed the data at the source.
// MAGIC 
// MAGIC #### Luckily with TIME TRAVEL you can easily create a view for your Data Science team to replay the data exactly how it was previously.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW deviceRaw_PreUpdate
// MAGIC   AS 
// MAGIC   SELECT * FROM deviceRaw TIMESTAMP AS OF '2019-11-25T18:07:28.000+0000' --MAKE SURE TO COPY/PASTE Relevant timestamp here
// MAGIC   WHERE cn IN ("United States", "United Kingdom", "China", "Japan", "Brazil")
// MAGIC   ; 
// MAGIC 
// MAGIC SELECT * FROM deviceRaw_PreUpdate

// COMMAND ----------

// MAGIC %md ###UPSERTS (Merge Into) ###
// MAGIC 
// MAGIC The MERGE INTO statement allows you to merge a set of updates and insertions into an existing dataset. For example, the following statement takes a stream of updates and merges it into the trip_data_delta table. When there is already an event present with the same primary key containing , Databricks Delta updates the data column using the given expression. When there is no matching event, Databricks Delta adds a new row.

// COMMAND ----------

// MAGIC %md
// MAGIC #### To setup this part of lab, we are now creating a Battery status table where each row is a unique device

// COMMAND ----------

val deviceBatteryStatus = spark.sql("""
  SELECT battery_level, ip, deviceRaw.device_name 
  FROM deviceRaw
  INNER JOIN (
    SELECT device_name, MAX(timestamp) AS timestamp FROM deviceRaw GROUP BY device_name
  ) latest ON deviceRaw.device_name = latest.device_name AND deviceRaw.timestamp = latest.timestamp
""")

deviceBatteryStatus.write.format("delta").mode("overwrite").save("/delta/batteryStatus")

display(deviceBatteryStatus)

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS batteryStatus;
// MAGIC CREATE TABLE batteryStatus
// MAGIC USING delta
// MAGIC AS SELECT *
// MAGIC FROM delta.`/delta/batteryStatus`

// COMMAND ----------

// MAGIC %md
// MAGIC #### Now say we are streaming in new data and we need to update our battery status table so IT OPs knows when/where to change batteries

// COMMAND ----------

Seq(
  (3, "127.0.0.0", "A Hello World"),
  (-20, "9.8.8.8", "A Hello World 2")
)
.toDF("battery_level", "ip", "device_name").createOrReplaceTempView("newData")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM newData

// COMMAND ----------

// MAGIC %md
// MAGIC #### With simple MERGE commands we can update existing records and insert new records

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO batteryStatus
// MAGIC USING newData
// MAGIC ON batteryStatus.device_name = newData.device_name 
// MAGIC WHEN MATCHED THEN
// MAGIC   UPDATE SET
// MAGIC      batteryStatus.battery_level = newData.battery_level 
// MAGIC     ,batteryStatus.ip = newData.ip
// MAGIC WHEN NOT MATCHED
// MAGIC   THEN INSERT *

// COMMAND ----------

// MAGIC %md
// MAGIC #### Let us peek at the new rows that got inserted.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM batteryStatus ORDER BY device_name

// COMMAND ----------

// MAGIC %md
// MAGIC # GDPR
// MAGIC Now imagine that these devices somehow have the right to be forgotten and are protected under GDPR regulation (*Just Roleplay*)
// MAGIC 
// MAGIC There is a webapp where users can issue delete requests. The backend logs and sends all the delete requests to a single table. We now need to take this table and process the Delete requests.

// COMMAND ----------

//create some deleteRequest data
Seq(
  ("A Hello World"),
  ("A Hello World 2"),
  ("device-mac-100011QxHwwVgOra"),
  ("device-mac-100041lQOYr")
)
.toDF("device_name").createOrReplaceTempView("deleteRequests")

display(spark.sql("select * from deleteRequests"))

// COMMAND ----------

// MAGIC %md ###Delete###
// MAGIC 
// MAGIC As Delta Table maintains history of table versions, it supports deletion of records which cannot be done in normal SparkSQL Tables.

// COMMAND ----------

// MAGIC %sql
// MAGIC DELETE FROM batteryStatus WHERE device_name IN (
// MAGIC   SELECT DISTINCT device_name FROM deleteRequests
// MAGIC );

// COMMAND ----------

// MAGIC %md
// MAGIC #### OR MERGE

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO batteryStatus
// MAGIC USING deleteRequests
// MAGIC ON batteryStatus.device_name = deleteRequests.device_name 
// MAGIC WHEN MATCHED THEN
// MAGIC   DELETE

// COMMAND ----------

// MAGIC %md Lets take a peek after delete operation.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM batteryStatus ORDER BY device_name

// COMMAND ----------

// MAGIC %md
// MAGIC <img src="https://github.com/kywe665/HelpImageHost/blob/master/DeltaTuning.png?raw=true" alt="drawing" width="1000"/>

// COMMAND ----------

// MAGIC %md Read NYC Taxi Dataset from some Azure Blob Storage.

// COMMAND ----------

val storageAccountName = "taxinydata"
val containerName = "taxidata"
val storageAccountAccessKey = "tQe7Y/4XOeMmGcSOlAX3HMk3MSDxPzz80fGnX77VQev5jq9ObJGVa0wZvoerbZ2ozDmK9bsF/3cMs/K/asYCWg=="
spark.conf.set(s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageAccountAccessKey)
val path = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/yellow_tripdata_2017-01.csv"

val cabs = spark.read.option("header", true).csv(path)

// COMMAND ----------

// MAGIC %md Write parquet based table using cabs data

// COMMAND ----------

cabs.write.format("parquet").mode("overwrite").partitionBy("PULocationID").save("/tmp/cabs_parquet")
cabs.write.format("delta").mode("overwrite").partitionBy("PULocationID").save("/tmp/cabs_delta")

// COMMAND ----------

// MAGIC %md Next, we run a query that get top 10 Pickup Locations with highest fares.

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val cabs_parquet = spark.read.format("parquet").load("/tmp/cabs_parquet")

display(cabs_parquet.groupBy("PULocationID").agg(sum($"total_amount".cast(DoubleType)).as("sum")).orderBy($"sum".desc).limit(10))

// COMMAND ----------

// MAGIC %md OPTIMIZE the Databricks Delta table

// COMMAND ----------

display(spark.sql("DROP TABLE  IF EXISTS cabs"))

display(spark.sql("CREATE TABLE cabs USING DELTA LOCATION '/tmp/cabs_delta'"))
        
display(spark.sql("OPTIMIZE cabs ZORDER BY (payment_type)"))

// COMMAND ----------

// MAGIC %md Now rerun the query in `cmd 73` on Delta Table and observe the latency.

// COMMAND ----------

val cabs_delta = spark.read.format("delta").load("/tmp/cabs_delta")

display(cabs_delta.groupBy("PULocationID").agg(sum($"total_amount".cast(DoubleType)).as("sum")).orderBy($"sum".desc).limit(10))

// COMMAND ----------

// MAGIC %md The query over the Databricks Delta table runs much faster after OPTIMIZE is run. How much faster the query runs can depend on the configuration of the cluster you are running on, however should be **5-10X faster** compared to the standard table. 
// MAGIC 
// MAGIC In our scenario, the query on Parquet table took 22.8 seconds while the same query on Delta table took 4.98 seconds.

// COMMAND ----------

// MAGIC %md ##ACID TRANSACTIONAL GUARANTEE : CONCURRENCY CONTROL AND ISOLATION LEVELS##
// MAGIC 
// MAGIC Databricks Delta provides ACID transactional guarantees between reads and writes. This means that
// MAGIC  - Multiple writers, even if they are across multiple clusters, can simultaneously modify a dataset and see a consistent snapshot view of the table and there will be a serial order for these writes
// MAGIC  - Readers will continue to see the consistent snapshot view of the table that the spark job started with even when the table is modified during the job.
// MAGIC 
// MAGIC ###Optimistic Concurrency Control###
// MAGIC 
// MAGIC Delta uses [Optimistic Concurrency Control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) to provide transactional guarantees between writes. Under this mechanism, writers operate in three stages.
// MAGIC 
// MAGIC    - Read: A transactional write will start by reading (if needed) the latest available version of the table to identify which files need to be modified (that is, rewritten).
// MAGIC    - Write: Then it will stage all the changes by writing new data files.
// MAGIC    - Validate and commit: Finally, for committing the changes, in our optimistic concurrency protocol checks whether the proposed changes conflict with any other changes that may have been concurrently committed since the snapshot that was read. If there are no conflicts, then the all the staged changes will be committed as a new versioned snapshot, and the write operation succeeds. However, if there are conflicts, then the write operation will fail with a concurrent modification exception rather than corrupting the table as would happen with OSS Spark.
