
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    window, count, min as spark_min,
    max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField, StringType
)

spark = (
    SparkSession.builder
    .appName("S8-Prueba-Watermark")
    .master("local[2]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("eventId", StringType()),
    StructField("icao24", StringType()),
    StructField("eventTime", StringType()),
    StructField("source", StringType())
])

kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "s8-watermark-prueba")
    .option("startingOffsets", "earliest")
    .load()
)

eventos = (
    kafka
    .select(
        from_json(
            col("value").cast("string"), schema
        ).alias("data")
    )
    .select("data.*")
    .filter(col("source") == "prueba_s8")
    .withColumn(
        "eventTimestamp",
        to_timestamp("eventTime")
    )
    .filter(col("eventTimestamp").isNotNull())
)

# WATERMARK DE DOS MINUTOS
eventos_watermark = eventos.withWatermark(
    "eventTimestamp", "2 minutes"
)

# VENTANAS FIJAS DE UN MINUTO
ventanas = (
    eventos_watermark
    .groupBy(window("eventTimestamp", "1 minute"))
    .agg(
        count("*").alias("totalEventos"),
        spark_min("eventTimestamp").alias("primerEvento"),
        spark_max("eventTimestamp").alias("ultimoEvento")
    )
    .select(
        col("window.start").alias("inicio"),
        col("window.end").alias("fin"),
        "totalEventos",
        "primerEvento",
        "ultimoEvento"
    )
)

print("PRUEBA WATERMARK S8 INICIADA")
print("Ventanas: 1 minuto")
print("Watermark: 2 minutos")
print("Salida: append (ventanas finalizadas)")

query = (
    ventanas.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 30)
    .option(
        "checkpointLocation",
        "/workspace/artifacts/checkpoints/"
        "s8_watermark_prueba_v1"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Deteniendo watermark...")
finally:
    query.stop()
    spark.stop()
