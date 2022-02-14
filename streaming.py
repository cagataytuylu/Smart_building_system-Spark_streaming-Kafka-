""" Using Spark streaming, consume a Kafka office-input topic. 
Estimate the mobility information (with or without activity in the room) using your model.

If there is activity in the room, it should be produced to the office-activity subject;
if there is no activity, it should be produced to the office-no-activity topic. """

import findspark
import warnings

findspark.init("/opt/manual/spark")
warnings.simplefilter(action='ignore')

from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("kafka_streaming")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())
# pyspark.sql.utils.AnalysisException: Queries with streaming sources must be executed with writeStream.start();

spark.sparkContext.setLogLevel('ERROR')
checkpoint_dir = "file:///tmp/streaming/write_to_kafka"

# test_df_schema = "co2_value double, temp_value double,light_value double, humidity_value double, time timestamp,room string"


# Read from kafka office-input topic
lines = (spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "office-input")
         .load())

lines.printSchema()
# print(lines) # lines kontrol edildi
# deserialization çünkü kaynak kafka
lines2 = lines.selectExpr("CAST(value AS STRING)")
# print(lines2)

lines3 = lines2.withColumn("co2_value", F.trim((F.split(F.col("value"), ",")[0])).cast("double")) \
    .withColumn("temp_value", F.trim((F.split(F.col("value"), ",")[1])).cast("double")) \
    .withColumn("light_value", F.trim((F.split(F.col("value"), ",")[2])).cast("double")) \
    .withColumn("humidity_value", F.trim((F.split(F.col("value"), ",")[3])).cast("double")) \
    .withColumn("time", F.trim((F.split(F.col("value"), ",")[4])).cast("timestamp")) \
    .withColumn("room", F.trim((F.split(F.col("value"), ",")[5]))) \
    .drop("value", "key", "topic", "partition", "offset", "timestamp")

lines3.printSchema()

# rm -rf /tmp/streaming/write_to_kafka/*


# lines3.show(5)
# print(lines3)

# Operation Part

from pyspark.ml.pipeline import PipelineModel

pipeline_model_loaded = PipelineModel.load(
    "file:///home/train/atscale4/final_homework/atscale4_F_HW/saved_model/pipeline_model")
transformed_df = pipeline_model_loaded.transform(lines3)



def sep_func(df, batchId):
    df.cache()
    df.show(5)
    df_columns = df.columns
    office_activitiy = df.filter("prediction == 1")
    office_activitiy.withColumn("value", F.concat(F.col("prediction"))).selectExpr("CAST(value AS STRING)").write.format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("topic", "office-activity") \
                .save()

    office_no_activity = df.filter("prediction == 0")
    office_no_activity.withColumn("value", F.concat(F.col("prediction"))).selectExpr("CAST(value AS STRING)").write.format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("topic", "office-no-activity") \
                .save()

streamingQuery = transformed_df.writeStream \
    .foreachBatch(sep_func) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

streamingQuery.awaitTermination()

