// Databricks notebook source
// Import bits useed for declaring schemas and working with JSON data
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Define a schema for the data
//val schema = (new StructType).add("dropoff_latitude", StringType).add("dropoff_longitude", StringType).add("extra", //StringType).add("fare_amount", StringType).add("improvement_surcharge", StringType).add("lpep_dropoff_datetime", //StringType).add("lpep_pickup_datetime", StringType).add("mta_tax", StringType).add("passenger_count", StringType).add("payment_type", //StringType).add("pickup_latitude", StringType).add("pickup_longitude", StringType).add("ratecodeid", StringType).add("store_and_fwd_flag", //StringType).add("tip_amount", StringType).add("tolls_amount", StringType).add("total_amount", StringType).add("trip_distance", //StringType).add("trip_type", StringType).add("vendorid", StringType)
// Reproduced here for readability
val schema = (new StructType)
   .add("VendorID", StringType)
   .add("lpep_pickup_datetime", StringType)
   .add("lpep_dropoff_datetime", StringType)
   .add("store_and_fwd_flag", StringType)
   .add("ratecodeID", StringType)
   .add("PULocationID", StringType)
   .add("DOLocationID", StringType)
   .add("passenger_count", StringType)
   .add("trip_distance", StringType)
   .add("fare_amount", StringType)
   .add("extra", StringType)
   .add("mta_tax", StringType)
   .add("tip_amount", StringType)
   .add("tolls_amount", StringType)
   .add("ehail_fee", StringType)
   .add("improvement_surcharge", StringType)
   .add("total_amount", StringType)
   .add("payment_type", StringType)
   .add("trip_type", StringType)


println("Schema declared")

// COMMAND ----------

//import org.apache.kafka.common.security.plain.PlainLoginModule

// Update values as needed
val TOPIC = "eh1"
val BOOTSTRAP_SERVERS = "anildwaeventhub1.servicebus.windows.net:9093"
val EH_SASL = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://anildwaeventhub1.servicebus.windows.net/;SharedAccessKeyName=receive;SharedAccessKey=Zl2UcfXbPQooLqlSdEbqY87aT6YgurMM3DZyzm4/BEo=;EntityPath=eh1\";"


val df = spark.readStream
    .format("kafka")
    .option("subscribe", TOPIC)
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .load()

// COMMAND ----------

// write to console
//Use dataframe like normal (in this example, write to console)
val df_write = df.writeStream
    .outputMode("append")
    .format("console")
    .start()

// COMMAND ----------

kafkaStreamDF.select(from_json(col("value").cast("string"), schema) as "trip")

// COMMAND ----------

// MAGIC %md Change checkpoint location path if the streaming fails to start
// MAGIC "checkpointLocation", "/streamcheckpoint42"

// COMMAND ----------

kafkaStreamDF.select(from_json(col("value").cast("string"), schema) as "trip").select("trip.VendorID", 
"trip.lpep_pickup_datetime", 
"trip.lpep_dropoff_datetime", 
"trip.store_and_fwd_flag", 
"trip.ratecodeID", 
"trip.PULocationID", 
"trip.DOLocationID", 
"trip.passenger_count", 
"trip.trip_distance", 
"trip.fare_amount", 
"trip.extra", 
"trip.mta_tax", 
"trip.tip_amount", 
"trip.tolls_amount", 
"trip.ehail_fee", 
"trip.improvement_surcharge", 
"trip.total_amount", 
"trip.payment_type", 
"trip.trip_type").writeStream.format("parquet").option("path","/mnt/rawtaxidata1").option("checkpointLocation", "/streamcheckpoint42").option("failOnDataLoss","false").start.awaitTermination(30000)
println("Wrote data to file")

// COMMAND ----------

