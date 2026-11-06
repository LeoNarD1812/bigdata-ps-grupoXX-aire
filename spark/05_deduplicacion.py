
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType
)

spark = (
    SparkSession.builder
    .appName("S8-Deduplicacion-Aerea")
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
    .option("subscribe", "s8-dedup-prueba")
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
    .withColumn(
        "eventTimestamp",
        to_timestamp("eventTime")
    )
    .filter(col("eventId").isNotNull())
    .filter(col("eventTimestamp").isNotNull())
    .filter(col("source") == "prueba_s8")
)

# Mantener estado temporal para eliminar duplicados.
unicos = (
    eventos
    .withWatermark("eventTimestamp", "10 minutes")
    .dropDuplicatesWithinWatermark(["eventId"])
)

print("SPARK DEDUPLICACION INICIADA")

query = (
    unicos.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        "/workspace/artifacts/checkpoints/"
        "s8_deduplicacion_v1"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

try:
    while query.isActive:
        time.sleep(5)

        progreso = query.lastProgress

        if progreso:
            print("\nMETRICAS DEL MICROLOTE")
            print(
                "Entradas:",
                progreso.get("numInputRows")
            )

            for operador in progreso.get("stateOperators", []):
                print(
                    "Duplicados descartados:",
                    operador.get("numDroppedDuplicateRows", "No reportado")
                )
                print(
                    "Filas descartadas por watermark:",
                    operador.get("numRowsDroppedByWatermark", 0)
                )

except KeyboardInterrupt:
    print("Deteniendo deduplicacion...")

finally:
    query.stop()
    spark.stop()
