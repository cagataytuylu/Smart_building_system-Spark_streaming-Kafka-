import findspark

findspark.init("/opt/manual/spark/")

from pyspark.sql import SparkSession, functions as F
import pandas as pd
import warnings
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml import Pipeline

warnings.simplefilter(action='ignore')
# display
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Dataset Story #
# Bu veri seti, UC Berkeley'deki Sutardja Dai Salonu'nun (SDH) 4
# katındaki 51 odada konumlanmış 255 sensör zaman serisinden toplanmıştır.
# Bir binadaki bir odanın fiziksel özelliklerindeki paternleri araştırmak için
# kullanılabilir. Ayrıca, Nesnelerin İnterneti (IoT), sensör füzyon ağı veya zaman
# serisi görevleriyle ilgili deneyler için de kullanılabilir. Bu veri seti hem denetimli
# (sınıflandırma ve regresyon) hem de denetimsiz öğrenme (kümeleme) görevleri için uygundur.
#
# Her oda 5 tür ölçüm içerir:
# - CO2 konsantrasyonu,
# - humidity: oda hava nemi,
# - temperature: oda sıcaklığı,
# - light: parlaklık
# - PIR: hareket sensörü verileri
#
# Veri 23 Ağustos 2013 Cuma ile 31 Ağustos 2013 Cumartesi arasında bir haftalık bir
# süre boyunca toplanmıştır. Hareket sensörü (PIR) her 10 saniyede bir değer üretir.
# Kalan sensörler her 5 saniyede bir değer üretir. Her dosya içinde zaman damgası ve
# sensör değerini içerir.
#
# Pasif kızılötesi sensör (PIR sensörü), görüş alanındaki nesnelerden yayılan kızılötesi
# (IR) ışığı ölçen ve bir odadaki doluluğu ölçen elektronik bir sensördür. PIR verilerinin
# yaklaşık %6'sı sıfır değildir ve odanın doluluk durumunu gösterir. PIR verilerinin kalan
# %94'ü sıfırdır ve boş bir oda olduğunu gösterir.

"""
Görevler:
Opsiyon-1:
.............
1. Bu veri setini kullanarak CO2, humidity, temperature, light ve zaman bilgileri 
bilinen bir odada herhangi bir aktivite veya hareketlilik olup olmadığını tahmin eden 
bir makine öğrenmesi modeli geliştiriniz.

2. Data-generator ile model geliştirirken kullandığınız test verisini, hedef değişkeni
 (pir) hariç tutarak Kafka office-input adında bir topic'e produce ediniz.

3. Spark streaming ile Kafka office-input topiğini consume ediniz. Modelinizi kullanarak
 hareketlilik bilgisini tahmin ediniz (odada aktivite var veya yok).

4. Odada hareketlilik var ise bunu office-activity topiğine, hareketlilik yok ise 
office-no-activity topiğine produce ediniz.

"""

spark = SparkSession.builder \
    .appName("final_hw") \
    .master("local[2]") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')
# Read data from source

df = spark.read.format("csv") \
    .option("header", False) \
    .option("inferSchema", True) \
    .option("sep", ",") \
    .load("file:///home/train/atscale4/final_homework/KETI/*/*.csv") \
    .withColumn("file_name", F.input_file_name()) \
    .withColumn("_c0", F.to_timestamp("_c0")) \
    .withColumn("room", F.element_at(F.reverse(F.split(F.col("file_name"), "/")), 2)) \
    .withColumn("sensor", F.regexp_replace(F.element_at(F.reverse(F.split(F.col("file_name"), "/")), 1), ".csv", "")) \
    .withColumnRenamed("_c0", "time") \
    .withColumnRenamed("_c1", "value") \
    .drop("file_name")

df.show(n=10, truncate=False)
df.cache()

# check the data is read properly
# df.select("sensor", "room").groupBy("sensor").agg(F.count('room').alias("room")).show()
# df.select(F.min("time"), F.max("time")).show(2)
# df.select("room").distinct().count()
# 51

###Explatory data analysis###
# df.select("sensor").distinct().count()

# df.count()
# df.printSchema()

##### Divide sensor into dfs #####

df_pir = df.filter("sensor=='pir'") \
    .withColumn("pir_value", F.col("value")) \
    .withColumn("room_pir", F.col("room")) \
    .withColumn("time_pir", F.col("time")) \
    .drop("sensor", "value", "room", "time")

# df_pir.show(5)
df_pir.select("pir_value").show()

# df_pir.count()
# df_pir.select("room_pir").distinct().count()

df_light = df.filter("sensor=='light'") \
    .withColumn("light_value", F.col("value")) \
    .withColumn("room_light", F.col("room")) \
    .withColumn("time_light", F.col("time")) \
    .drop("sensor", "value", "room", "time")

# df_light.count()
# df_light.show(5)

df_humidity = df.filter("sensor=='humidity'") \
    .withColumn("humidity_value", F.col("value")) \
    .withColumn("room_humidity", F.col("room")) \
    .withColumn("time_humidity", F.col("time")) \
    .drop("sensor", "value", "room", "time")

# df_humidity.show(5)
# df_humidity.count()

df_c02 = df.filter("sensor=='co2'") \
    .withColumn("co2_value", F.col("value")) \
    .withColumn("room_co2", F.col("room")) \
    .withColumn("time_co2", F.col("time")) \
    .drop("sensor", "value", "room", "time")

# df_c02.show(5)
# df_c02.count()

df_temp = df.filter("sensor=='temperature'") \
    .withColumn("temp_value", F.col("value")) \
    .withColumn("room_temp", F.col("room")) \
    .withColumn("time_temp", F.col("time")) \
    .drop("sensor", "value", "room", "time")

# df_temp.show(5)
# df_temp.count()

# join dfs

df_all_v3 = df_pir.join(df_c02, (df_pir["room_pir"] == df_c02["room_co2"]) & (df_pir["time_pir"] == df_c02["time_co2"]),
                        "inner") \
    .join(df_temp, (df_pir["room_pir"] == df_temp["room_temp"]) & (df_pir["time_pir"] == df_temp["time_temp"]), "inner") \
    .join(df_light, (df_pir["room_pir"] == df_light["room_light"]) & (df_pir["time_pir"] == df_light["time_light"]),
          "inner") \
    .join(df_humidity,
          (df_pir["room_pir"] == df_humidity["room_humidity"]) & (df_pir["time_pir"] == df_humidity["time_humidity"]),
          "inner")

# df_all_v3.count()
# 5165174
# df_all_v3.show(6)

# sensor de droplanabilir , kontrol amaçlı bıraktım
df_all = df_all_v3 \
    .withColumn("time", F.col("time_pir")) \
    .withColumn("room", F.col("room_pir")) \
    .drop("value", "room_co2", "time_co2", "room_temp", "time_temp", "room_light", "time_light", "room_humidity",
          "time_humidity", "time_pir", "room_pir")

# df_all.show(10)
# df_all.count()
# 135386
"""
 |pir_value|co2_value|temp_value|light_value|humidity_value|               time|room|
+---------+---------+----------+-----------+--------------+-------------------+----+
|     28.0|    688.0|     23.57|       47.0|         55.14|2013-08-26 22:45:11| 415|
|     30.0|    881.0|     24.12|       55.0|         54.95|2013-08-26 23:47:46| 415|
|     30.0|    699.0|     23.94|       54.0|          54.7|2013-08-27 00:13:21| 415|
|      0.0|    559.0|      24.0|       64.0|          54.8|2013-08-28 03:56:23| 415|
|      0.0|    509.0|      23.8|       79.0|         55.05|2013-08-28 04:48:43| 415|
|      0.0|    482.0|     23.73|       30.0|         55.48|2013-08-28 05:22:53| 415|
|      0.0|    465.0|      23.5|        4.0|         56.23|2013-08-28 06:19:13| 415|
|      0.0|    478.0|      23.4|        1.0|         56.85|2013-08-28 06:55:03| 415|
|      0.0|    465.0|     23.32|        4.0|         57.16|2013-08-28 07:21:23| 415|
|      0.0|    468.0|     23.27|        4.0|         57.44|2013-08-28 08:07:33| 415|
+---------+---------+----------+-----------+--------------+-------------------+----+
 
 """

# df_all.printSchema()

#####Summary Statistics#####

# df_all.select("pir_value","co2_value", "temp_value", "light_value", "humidity_value").describe().show()
"""
                                                                               
|summary|         co2_value|        temp_value|       light_value|    humidity_value|
+-------+------------------+------------------+------------------+------------------+
|  count|            135386|            135386|            135386|            135386|
|   mean|399.27423071809494| 23.41589477493978|140.57186119687412| 56.89176598762056|
| stddev|119.08185828746514|11.836374214777692| 470.6867628720149|3.6119981360625135|
|    min|              62.0|              20.2|               0.0|             42.12|
|    max|            1223.0|            579.27|            2397.0|             71.29|
+-------+------------------+------------------+------------------+------------------+

"""
###### Null Check#####
# null olmaması gerekir
df_all_count = df_all.count()

for (col_name, col_type) in zip(df_all.columns, df_all.dtypes):
    null_count = df_all.filter((F.col(col_name).isNull()) | (F.col(col_name) == "")).count()
    if (null_count > 0):
        print("{} {} type has {} null values, % {}".format(col_name, col_type[1], null_count,
                                                           (null_count / df_all_count)))

##### Target feature #####

# df_all.select("pir_value").groupBy("pir_value").count().show()


df_new = df_all.withColumn("pir_value", F.when(F.col("pir_value") > 0, 1) \
                           .otherwise(F.col("pir_value"))) \
    .withColumn("label", F.col("pir_value").cast("int")).drop("pir_value")

# control new df
# df_new.show(10)

"""
|pir_value|co2_value|temp_value|light_value|humidity_value|               time|room|
+---------+---------+----------+-----------+--------------+-------------------+----+
|      1.0|    688.0|     23.57|       47.0|         55.14|2013-08-26 22:45:11| 415|
|      1.0|    881.0|     24.12|       55.0|         54.95|2013-08-26 23:47:46| 415|
|      1.0|    699.0|     23.94|       54.0|          54.7|2013-08-27 00:13:21| 415|
|      0.0|    559.0|      24.0|       64.0|          54.8|2013-08-28 03:56:23| 415|
+---------+---------+----------+-----------+--------------+-------------------+----+


"""
# df_new.printSchema()


# df_new.select("pir_value").groupBy("pir_value").count().show()
"""
                                                                                +---------+------+
|pir_value| count|
+---------+------+
|      1.0|  9097|
|      0.0|126289|
+---------+------+

"""
9097 / 135386  # %6 sı sıfır değil buradan da anlaşıldığı üzere df doğrudur

##### Define Cols #####
# label_col = ["pir_value"]

# room değişkeni 51 kategori içerir o sebeple one-hot-encoderdan geçmesi gerekir tabi öncesinde stringindexer dan.

#####StringIndexer#####
string_indexer_objs = StringIndexer(inputCol="room",
                                    outputCol="roomIdx",
                                    handleInvalid='error')

#####One Hot Encoder#####
encoder = OneHotEncoder(inputCols=["roomIdx"],
                        outputCols=["ohe_col"],
                        handleInvalid='error')

#####Vector Assembler#####
# vector assembler içerisinde target olmamalıdır
assembler = VectorAssembler(inputCols=['co2_value', "temp_value", "light_value", "humidity_value", 'ohe_col'],
                            outputCol='features',
                            handleInvalid='skip')

##### LabelIndexer #####
# label_indexer = StringIndexer().setHandleInvalid("skip") \
#    .setInputCol(label[0]) \
#    .setOutputCol("label")

##### Model #####
# Estimator

estimator = GBTClassifier() \
    .setFeaturesCol("features") \
    .setLabelCol("label")
# Pipeline

pipeline_obj = Pipeline().setStages([string_indexer_objs, encoder, assembler, estimator])

train_df, test_df = df_new.randomSplit([.8, .2], seed=142)

## test_df den label çıkartılıp yapılan deneme


test_df.show(5)
# test_df2 = test_df.withColumn("pir_value", F.lit(""))
# test_df2.show()
# test_df3 = test_df2.withColumn("pir_value", F.lit(""))
# test_df3.show(5)
"""
#Split time series
from pyspark.sql.functions import percent_rank
from pyspark.sql import Window

df_new = df_new.withColumn("rank", percent_rank().over(Window.partitionBy().orderBy("time")))

train_df = df_new.where("rank <= .8").drop("rank")
#train_df.show(3)
#2013-08-24 02:04:53 - (2013-8-30 08:51:13)
#train_df.select("room").distinct().count()
#51

test_df = df_new.where("rank > .2").drop("rank")
#test_df.show(3)
#2013-08-30 08:51:14 - 2013-8-31 21:23:29
test_df.select("room").distinct().count()
#49"""

pipeline_model = pipeline_obj.fit(train_df)
transformed_df = pipeline_model.transform(test_df)

# Prediction
transformed_df.show(truncate=False)

# Evaluate the Model
from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator()
evaluator.evaluate(transformed_df)
# 0.9810528895991124
evaluator.getMetricName()
# 'areaUnderROC'

# check overfitting
# df_new.select("pir_value", "co2_value", "temp_value", "light_value", "humidity_value").groupBy("pir_value")\
#    .agg(F.mean("co2_value"),F.mean("temp_value"), F.mean("light_value"), F.mean("humidity_value")) \
#    .show()
"""
|pir_value|   avg(co2_value)|   avg(temp_value)|  avg(light_value)|avg(humidity_value)|
+---------+-----------------+------------------+------------------+-------------------+
|      1.0|550.1581840167088|26.421822578872163|173.20907991645598|  55.76405298450042|
|      0.0|388.4055776829336| 23.19936819517139|138.22089809880512|  56.97299875681961|
+---------+-----------------+------------------+------------------+-------------------+"""

##### save model to disk
pipeline_model.write().overwrite().save(
    "file:///home/train/atscale4/final_homework/atscale4_F_HW/saved_model/pipeline_model")

from pyspark.ml.pipeline import PipelineModel

pipeline_model_loaded = PipelineModel.load(
    "file:///home/train/atscale4/final_homework/atscale4_F_HW/saved_model/pipeline_model")

#####Kafka Kısmı için hazırlık #####
# test_df için bir dosya hazırlandı ve oraya yazıldı
test_df.coalesce(1).write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save("file:///home/train/atscale4/final_homework/test_df")

# Kafka kısmını terminalden çalıştırabilirsiniz
