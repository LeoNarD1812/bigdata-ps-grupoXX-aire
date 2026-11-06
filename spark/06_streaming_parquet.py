
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

# ==========================================
# 1. CONFIGURACION
# ==========================================

KAFKA_SERVER = "kafka:9092"
TOPIC = "aeronaves-telemetria"

PAQUETE_KAFKA = (
    "org.apache.spark:"
    "spark-sql-kafka-0-10_2.13:4.2.0"
)

RUTA_PARQUET = (
    "/workspace/artifacts/s8/parquet_aeronaves"
)

RUTA_CHECKPOINT = (
    "/workspace/artifacts/checkpoints/"
    "s8_parquet_aeronaves_v1"
)

# ==========================================
# 2. INICIAR SPARK
# ==========================================

spark = (
    SparkSession.builder
    .appName("S8-Streaming-Parquet-Aereo")
    .master("local[2]")
    .config("spark.jars.packages", PAQUETE_KAFKA)
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("SPARK INICIADO:", spark.version)

# ==========================================
# 3. ESQUEMA DE TELEMETRIA
# ==========================================

schema = StructType([
    StructField("eventId", StringType()),
    StructField("eventType", StringType()),
    StructField("schemaVersion", IntegerType()),
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("eventTime", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("baroAltitude", DoubleType()),
    StructField("velocity", DoubleType()),
    StructField("verticalRate", DoubleType()),
    StructField("source", StringType())
])

# ==========================================
# 4. LECTURA CONTINUA DE KAFKA
# ==========================================

kafka = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_SERVER
    )
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .load()
)

# ==========================================
# 5. TRANSFORMACION Y VALIDACION
# ==========================================

eventos = (
    kafka
    .select(
        col("partition").alias("kafkaPartition"),
        col("offset").alias("kafkaOffset"),
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select(
        "kafkaPartition",
        "kafkaOffset",
        "data.*"
    )
    .withColumn(
        "eventTimestamp",
        to_timestamp("eventTime")
    )

    # Contrato
    .filter(col("eventType") == "aeronave.telemetria")
    .filter(col("schemaVersion") == 1)
    .filter(col("source") == "opensky")
    .filter(col("icao24").rlike("^[0-9a-f]{6}$"))
    .filter(col("eventId").isNotNull())
    .filter(col("eventTimestamp").isNotNull())

    # Coordenadas
    .filter(col("latitude").between(-90, 90))
    .filter(col("longitude").between(-180, 180))

    # Altitud
    .filter(
        col("baroAltitude").isNull() |
        col("baroAltitude").between(-500, 20000)
    )

    # Velocidad
    .filter(
        col("velocity").isNull() |
        col("velocity").between(0, 400)
    )

    # Velocidad vertical
    .filter(
        col("verticalRate").isNull() |
        col("verticalRate").between(-100, 100)
    )
)

# ==========================================
# 6. COLUMNAS PARA PARQUET
# ==========================================

resultado = eventos.select(
    "eventId",
    "icao24",
    "callsign",
    "eventTimestamp",
    "latitude",
    "longitude",
    "baroAltitude",
    "velocity",
    "verticalRate",
    "source",
    "kafkaPartition",
    "kafkaOffset"
)

# ==========================================
# 7. ESCRITURA STREAMING EN PARQUET
# ==========================================

print("\nINICIANDO PERSISTENCIA PARQUET...")
print("TOPIC:", TOPIC)
print("DESTINO:", RUTA_PARQUET)
print("CHECKPOINT:", RUTA_CHECKPOINT)

query = (
    resultado.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", RUTA_PARQUET)
    .option(
        "checkpointLocation",
        RUTA_CHECKPOINT
    )
    .trigger(processingTime="10 seconds")
    .queryName("persistencia_aeronaves")
    .start()
)

# ==========================================
# 8. METRICAS DEL STREAMING
# ==========================================

ultimo_batch = None

try:
    while query.isActive:
        time.sleep(5)

        progreso = query.lastProgress

        if progreso:
            batch_id = progreso.get("batchId")

            if batch_id != ultimo_batch:
                ultimo_batch = batch_id

                print("\nMICROLOTE COMPLETADO")

                metricas = {
                    "batchId": batch_id,
                    "numInputRows": progreso.get(
                        "numInputRows"
                    ),
                    "inputRowsPerSecond": progreso.get(
                        "inputRowsPerSecond"
                    ),
                    "processedRowsPerSecond": progreso.get(
                        "processedRowsPerSecond"
                    ),
                    "durationMs": progreso.get(
                        "durationMs"
                    )
                }

                print(json.dumps(
                    metricas,
                    indent=2,
                    ensure_ascii=False
                ))

except KeyboardInterrupt:
    print("\nDETENIENDO STREAMING...")

finally:
    query.stop()
    spark.stop()
    print("SPARK FINALIZADO")
